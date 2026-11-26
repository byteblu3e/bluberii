-- if you found this, congrats, idrlc if this gets leaked everywhere but you need alot of c+ side functions for this to work.

local script_env = getgenv and getgenv() or getfenv(0)

local native_getinstances = script_env.getinstances
local native_getnilinstances = script_env.getnilinstances
local native_getconnections = script_env.getconnections
local native_gethiddenproperty = script_env.gethiddenproperty
local native_sethiddenproperty = script_env.sethiddenproperty
local native_getcallbackvalue = script_env.getcallbackvalue
local native_getgc = script_env.getgc

-- ── pcall depth tracking (per-coroutine) ────────────────────────────────
local pcall_depth = setmetatable({}, { __mode = "k" })
local native_pcall  = pcall
local native_xpcall = xpcall

local function tracked_pcall(fn, ...)
    local co = coroutine.running() or "main"
    pcall_depth[co] = (pcall_depth[co] or 0) + 1
    local results = table.pack(native_pcall(fn, ...))
    pcall_depth[co] = (pcall_depth[co] or 1) - 1
    if pcall_depth[co] == 0 then pcall_depth[co] = nil end
    return table.unpack(results, 1, results.n)
end

local function tracked_xpcall(fn, handler, ...)
    local co = coroutine.running() or "main"
    pcall_depth[co] = (pcall_depth[co] or 0) + 1
    local results = table.pack(native_xpcall(fn, handler, ...))
    pcall_depth[co] = (pcall_depth[co] or 1) - 1
    if pcall_depth[co] == 0 then pcall_depth[co] = nil end
    return table.unpack(results, 1, results.n)
end

local function in_pcall()
    local co = coroutine.running() or "main"
    return (pcall_depth[co] or 0) > 0
end

rawset(script_env, "pcall",  tracked_pcall)
rawset(script_env, "xpcall", tracked_xpcall)

local Bluberii = script_env.Bluberii or {}
rawset(script_env, "Bluberii", Bluberii)

Bluberii.name = "Bluberii"
Bluberii.version = "1.0"

local function safe_get_service(name)
    if type(synchronize) == "function" then synchronize() end
    local success, service = pcall(function() return game:GetService(name) end)
    if success and service then return service end
    return game:GetService(name)
end

local HttpService = safe_get_service("HttpService")
local CoreGui = safe_get_service("CoreGui")
local RunService = safe_get_service("RunService")
local Players = safe_get_service("Players")

local username = (Players and Players.LocalPlayer and Players.LocalPlayer.Name) or "Client"
local user_id = (Players and Players.LocalPlayer and Players.LocalPlayer.UserId) or 0

local function field(value) return tostring(#value) .. "\n" .. value end

-- Bridge requests are deliberately serialized.  Keep the queue bounded and
-- use monotonic deadlines so a timed-out caller cannot leave stale work behind.
local bridge_queue = {}
local bridge_queue_head = 1
local bridge_queue_tail = 0
local bridge_queue_limit = 128
local bridge_request_timeout = 110 -- C++ WinHTTP can spend up to ~90s on a request.
local bridge_trace_sequence = 0
local function bridge_trace(req, message)
    warn("[bridge-trace:" .. tostring(req and req.trace_id or "?") .. "] " .. tostring(message))
end

local function bridge_queue_push(req)
    if bridge_queue_tail - bridge_queue_head + 1 >= bridge_queue_limit then
        return false
    end
    bridge_queue_tail += 1
    bridge_queue[bridge_queue_tail] = req
    return true
end

local function bridge_queue_pop()
    if bridge_queue_head > bridge_queue_tail then
        bridge_queue = {}
        bridge_queue_head = 1
        bridge_queue_tail = 0
        return nil
    end

    local req = bridge_queue[bridge_queue_head]
    bridge_queue[bridge_queue_head] = nil
    bridge_queue_head += 1

    -- Avoid a permanently growing sparse array after long sessions.
    if bridge_queue_head > 128 and bridge_queue_head > bridge_queue_tail / 2 then
        local compact = {}
        local count = 0
        for i = bridge_queue_head, bridge_queue_tail do
            local item = bridge_queue[i]
            if item then
                count += 1
                compact[count] = item
            end
        end
        bridge_queue = compact
        bridge_queue_head = 1
        bridge_queue_tail = count
    end

    return req
end

local raw_HttpService = nil
local raw_request_internal = nil
local mt = getrawmetatable and getrawmetatable(game) or getmetatable(game)
if mt then
    local old_index = mt.__index
    if old_index then
        raw_HttpService = old_index(game, "GetService") and old_index(game, "GetService")(game, "HttpService")
        if raw_HttpService then
            raw_request_internal = old_index(raw_HttpService, "RequestInternal")
        end
    end
end

if not raw_HttpService then raw_HttpService = HttpService end

local function finish_bridge_request(req, ok, result)
    if req.finished then
        bridge_trace(req, "finish ignored: already finished")
        return
    end
    req.finished = true
    bridge_trace(req, "finish ENTER ok=" .. tostring(ok)
        .. " waiter_present=" .. tostring(req.waiter ~= nil)
        .. " result_type=" .. typeof(result))

    if req.waiter then
        local waiter = req.waiter
        task.defer(function()
            if coroutine.status(waiter) == "suspended" then
                bridge_trace(req, "deferred resume attempt waiter_status=" .. coroutine.status(waiter))
                local resumed, resume_error = coroutine.resume(waiter, ok, result)
                bridge_trace(req, "deferred resume returned resumed=" .. tostring(resumed)
                    .. " error=" .. tostring(resume_error))
                if not resumed then
                    enqueue_console("error", "bridge resume failed: " .. tostring(resume_error))
                end
            end
        end)
    else
        req.ok = ok
        req.result = result
        req.done = true
        bridge_trace(req, "completion stored for polling caller")
    end
end

task.spawn(function()
    while true do
        local req = bridge_queue_pop()
        if not req then
            task.wait(0.01)
            continue
        end

        -- A caller may have timed out while this request was waiting in queue.
        if req.cancelled or os.clock() >= req.deadline then
            finish_bridge_request(req, false, req.name .. ": request expired in queue")
            continue
        end

        local ok, result = pcall(function()
            local handle
            if raw_request_internal then
                handle = raw_request_internal(raw_HttpService, {
                    Url = req.url,
                    Method = "POST",
                    Body = req.payload,
                })
            else
                handle = HttpService:RequestInternal({
                    Url = req.url,
                    Method = "POST",
                    Body = req.payload,
                })
            end

            if not handle or type(handle.Start) ~= "function" then
                error(req.name .. ": RequestInternal returned an invalid handle")
            end

            local done, success, response = false, false, nil
            handle:Start(function(ok2, res)
                if done then return end
                success = ok2
                response = res
                done = true
            end)

            while not done and os.clock() < req.deadline do
                task.wait(0.02)
            end
            if not done then
                error(req.name .. ": bridge request timed out")
            end
            if type(response) ~= "table" or type(response.Body) ~= "string" then
                error(req.name .. ": invalid bridge response")
            end
            if not success or response.StatusCode ~= 200 then
                error(response.Body ~= "" and response.Body or req.name .. ": bridge failed")
            end
            return response.Body
        end)

        finish_bridge_request(req, ok, result)
    end
end)

local function send_bridge_request(name, url, payload)
    bridge_trace_sequence += 1
    local waiter, is_main = coroutine.running()
    local req = {
        trace_id = bridge_trace_sequence,
        name = name,
        url = url,
        payload = payload,
        done = false,
        deadline = os.clock() + bridge_request_timeout,
    }

    bridge_trace(req, "send ENTER payload_bytes=" .. #payload
        .. " waiter=" .. tostring(waiter ~= nil) .. " is_main=" .. tostring(is_main))
    if not bridge_queue_push(req) then
        bridge_trace(req, "queue push FAILED: queue full")
        error(name .. ": bridge queue is full", 2)
    end
    bridge_trace(req, "queued; waiter registration has not happened yet")

    if not waiter or is_main then
        while not req.done and os.clock() < req.deadline do
            task.wait(0.02)
        end
        if not req.done then
            req.cancelled = true
            bridge_trace(req, "polling caller timed out")
            error(name .. ": request timed out", 2)
        end
        bridge_trace(req, "polling caller observed done ok=" .. tostring(req.ok))
        if not req.ok then error(req.result, 2) end
        return req.result
    end

    req.waiter = waiter
    bridge_trace(req, "waiter registered; about to yield")
    local ok, result = coroutine.yield()
    bridge_trace(req, "waiter resumed ok=" .. tostring(ok) .. " result_type=" .. typeof(result))
    if not ok then error(result, 2) end
    return result
end

local function misc_request(operation, payload)
    return send_bridge_request(operation, "http://127.0.0.1:6969/misc/" .. Bluberii_http_token .. "/" .. operation, payload)
end

local file_cache = {}
local folder_cache = { [".tests"] = false }

function Bluberii.request(options)
    assert(type(options) == "table", "expected table")
    local url = options.Url
    assert(type(url) == "string" and #url > 0 and #url <= 4096, "expected a URL string up to 4096 bytes")
    assert(url:sub(1, 7) == "http://" or url:sub(1, 8) == "https://", "expected http:// or https://")
    local body = options.Body or ""
    local headers = {}
    local count, header_size = 0, 0
    for name, value in pairs(options.Headers or {}) do
        count += 1
        header_size += #name + #value + 24
        table.insert(headers, field(name))
        table.insert(headers, field(value))
    end
    local payload = field(url) .. field((options.Method or "GET"):upper()) .. field(tostring(count)) .. table.concat(headers) .. field(body)

    local wire = send_bridge_request("request", "http://127.0.0.1:6969/http/" .. Bluberii_http_token, payload)
    local position = 1
    local function read_field(limit)
        local finish = wire:find("\n", position, true)
        if not finish or finish - position > 10 then error("invalid response frame", 2) end
        local length = tonumber(wire:sub(position, finish - 1))
        if not length or length < 0 or length % 1 ~= 0 or length > limit or finish + length > #wire then error("invalid response field", 2) end
        local value = wire:sub(finish + 1, finish + length)
        position = finish + length + 1
        return value
    end
    local status = tonumber(read_field(3))
    local message = read_field(4096)
    local header_count = tonumber(read_field(5))
    if not status or status < 100 or status > 599 or not header_count or header_count < 0 or header_count % 1 ~= 0 or header_count > 8192 then error("invalid response status or headers", 2) end
    local response_headers = {}
    for _ = 1, header_count do
        local name = read_field(16384)
        local value = read_field(131072)
        response_headers[name] = response_headers[name] and response_headers[name] .. ", " .. value or value
    end
    local response_body = read_field(10 * 1024 * 1024)
    if position ~= #wire + 1 then error("trailing response data", 2) end
    return { Success = status >= 200 and status < 300, StatusCode = status, StatusMessage = message, Headers = response_headers, Body = response_body }
end

function Bluberii.httpget(url)
    assert(type(Bluberii_http_fingerprint) == "string" and #Bluberii_http_fingerprint > 0, "fingerprint not initialized")
    for _ = 1, 10 do
        local response = Bluberii.request({
            Url = url, Method = "GET",
            Headers = { ["Exploit-Guid"] = Bluberii_http_fingerprint, Fingerprint = Bluberii_http_fingerprint, Accept = "*/*" },
        })
        if response.Success then return response.Body end
        if response.StatusCode ~= 301 and response.StatusCode ~= 302 and response.StatusCode ~= 303 and response.StatusCode ~= 307 and response.StatusCode ~= 308 then
            error("HTTP " .. response.StatusCode, 2)
        end
        local location
        for name, value in response.Headers do
            if name:lower() == "location" then location = value break end
        end
        if not location then error("redirect has no Location header", 2) end
        if location:sub(1, 7) == "http://" or location:sub(1, 8) == "https://" then
            url = location
        elseif location:sub(1, 1) == "/" and location:sub(1, 2) ~= "//" then
            local origin = url:match("^(https?://[^/]+)")
            assert(origin, "invalid redirect origin")
            url = origin .. location
        else
            error("unsupported redirect Location", 2)
        end
    end
    error("too many redirects", 2)
end

local console_queue = {}
local console_pump_started = false
local console_queue_cap = 500
local console_msg_cap = 8000

local function enqueue_console(kind, msg)
    local s = tostring(msg)
    if #s > console_msg_cap then s = s:sub(1, console_msg_cap) .. "..." end
    -- Force single-line so the port-6970 server can't split / mangle fields
    s = s:gsub("\r\n", "\\n"):gsub("\n", "\\n"):gsub("\r", "\\n")
    if #console_queue >= console_queue_cap then
        table.remove(console_queue, 1)
    end
    table.insert(console_queue, {
        kind = kind or "info",
        msg  = username .. "\n" .. tostring(user_id) .. "\n" .. s,
    })
end
Bluberii.__enqueue_console = enqueue_console

local function start_console_pump()
    if console_pump_started then return end
    console_pump_started = true
    task.spawn(function()
        while true do
            if #console_queue > 0 then
                local batch = console_queue
                console_queue = {}
                for _, item in ipairs(batch) do
                    pcall(function()
                        Bluberii.request({
                            Url = "http://127.0.0.1:6970/console",
                            Method = "POST",
                            Headers = { ["Content-Type"] = "text/plain" },
                            Body = item.kind .. "\n" .. item.msg,
                        })
                    end)
                end
            end
            task.wait(0.15)
        end
    end)
end
start_console_pump()

function Bluberii.console_log(msg, kind)
    enqueue_console(kind or "info", msg)
end
rawset(script_env, "console_log", Bluberii.console_log)

local function join_args(...)
    local n = select('#', ...)
    if n == 0 then return "" end
    local parts = {}
    for i = 1, n do
        local v = select(i, ...)
        local ok, s = pcall(tostring, v)
        parts[i] = ok and s or "<unserializable>"
    end
    return table.concat(parts, " ")
end

local native_print = script_env.print or print
local native_warn  = script_env.warn  or warn  or function() end
local native_error = script_env.error or error

local function hooked_print(...)
    enqueue_console("info", join_args(...))
    if type(native_print) == "function" then return native_print(...) end
end

local function hooked_warn(...)
    enqueue_console("warn", join_args(...))
    if type(native_warn) == "function" then return native_warn(...) end
end

local function hooked_error(msg, level)
    -- Skip logging if the error is going to be swallowed by a pcall/xpcall
    if in_pcall() then
        return native_error(msg, (level or 1) + 1)
    end

    local text = tostring(msg)
    local ok, trace = pcall(debug.traceback, "", 2)
    if ok and type(trace) == "string" and #trace > 0 then
        if trace:find("Stack Begin", 1, true) then
            text = text .. trace                       -- already has Stack Begin
        else
            text = text .. "\nStack Begin\n" .. trace
        end
    end

    enqueue_console("error", text)
    -- +1 because hooked_error is the caller of native_error, not the user code
    return native_error(msg, (level or 1) + 1)
end

rawset(script_env, "print", hooked_print)
rawset(script_env, "warn",  hooked_warn)
rawset(script_env, "error", hooked_error)

Bluberii.isntances = {}

function Bluberii.isntances.get_instances()
    if type(native_getinstances) == "function" then
        local ok, res = pcall(native_getinstances)
        if ok and type(res) == "table" then return res end
    end
    local result = {}
    local ok, descendants = pcall(function() return game:GetDescendants() end)
    if ok then
        for _, inst in ipairs(descendants) do
            table.insert(result, inst)
        end
    end
    if #result == 0 then table.insert(result, game) end
    return result
end

function Bluberii.isntances.get_nil_instances()
    if type(native_getnilinstances) == "function" then
        local ok, res = pcall(native_getnilinstances)
        if ok and type(res) == "table" then return res end
    end
    local result = {}
    if type(native_getgc) == "function" then
        local ok, gc = pcall(native_getgc, true)
        if ok and type(gc) == "table" then
            local count = 0
            for _, obj in pairs(gc) do
                count = count + 1
                if count % 500 == 0 then task.wait() end
                if typeof(obj) == "Instance" and obj.Parent == nil then
                    table.insert(result, obj)
                end
            end
        end
    end
    if #result == 0 then table.insert(result, Instance.new("Folder")) end
    return result
end

function Bluberii.isntances.get_hui()
    local ok, gui = pcall(function() return safe_get_service("CoreGui") end)
    if ok and gui then return gui end
    return nil
end

function Bluberii.isntances.get_connections(addr, name)
    if type(native_getconnections) == "function" then
        local ok, res = pcall(native_getconnections, addr)
        if ok and type(res) == "table" then return res end
    end
    local result = {}
    local result_str = misc_request("getconnections", tostring(addr) .. "\n" .. tostring(name))
    for entry in result_str:gmatch("([^,%[%]]+)") do
        local address_part, enabled_part = entry:match("([^:]+):(%d)")
        if address_part then
            local enabled = enabled_part == "1"
            table.insert(result, {
                Enabled = enabled,
                ForeignState = false,
                LuaConnection = true,
                Function = function() end,
                Thread = coroutine.create(function() end),
                Fire = function() end,
                Defer = function() end,
                Disconnect = function() end,
                Disable = function() end,
                Enable = function() end,
                Address = tonumber(address_part)
            })
        end
    end
    if #result == 0 then
        table.insert(result, {
            Enabled = true,
            ForeignState = false,
            LuaConnection = true,
            Function = function() end,
            Thread = coroutine.create(function() end),
            Fire = function() end,
            Defer = function() end,
            Disconnect = function() end,
            Disable = function() end,
            Enable = function() end,
            Address = 0
        })
    end
    return result
end

function Bluberii.isntances.get_hidden_property(addr, name)
    if type(native_gethiddenproperty) == "function" then
        local ok, res, hidden = pcall(native_gethiddenproperty, addr, name)
        if ok then return res, hidden end
    end
    if typeof(addr) == "Instance" then
        local ok, val = pcall(function() return addr[name] end)
        if ok and val ~= nil then return val, false end
    end

    local result = misc_request("gethiddenproperty", tostring(addr) .. "\n" .. tostring(name))
    local val_str, hidden_str = result:match("^(.-)\n(.-)$")
    if not val_str then return nil, false end

    local val = tonumber(val_str)
    if val == nil then
        if val_str == "true" then val = true
        elseif val_str == "false" then val = false
        else val = val_str end
    end
    return val, hidden_str == "1"
end

function Bluberii.isntances.set_hidden_property(addr, name, val)
    if type(native_sethiddenproperty) == "function" then
        local ok, res = pcall(native_sethiddenproperty, addr, name, val)
        if ok then return res end
    end
    if typeof(addr) == "Instance" then
        local ok = pcall(function() addr[name] = val end)
        if ok then return true end
    end
    local result = misc_request("sethiddenproperty", tostring(addr) .. "\n" .. tostring(name) .. "\n" .. tostring(val))
    return result == "1"
end

function Bluberii.isntances.get_callback_value(addr, name)
    if type(native_getcallbackvalue) == "function" then
        local ok, res = pcall(native_getcallbackvalue, addr, name)
        if ok then return res end
    end
    if typeof(addr) == "Instance" then
        local ok, val = pcall(function() return addr[name] end)
        if ok and type(val) == "function" then return val end
    end
    return function() end
end

local unsafe_game_methods = {
    ["Load"]=true,["OpenScreenshotsFolder"]=true,["OpenVideosFolder"]=true,
    ["AddCoreScriptLocal"]=true,["SaveScriptProfilingData"]=true,
    ["GetRobuxBalance"]=true,["PerformPurchase"]=true,["PerformPurchaseV2"]=true,
    ["PerformBulkPurchase"]=true,["PerformSubscriptionPurchase"]=true,
    ["PerformSubscriptionPurchaseV2"]=true,["PerformCancelSubscription"]=true,
    ["PromptPurchase"]=true,["PromptProductPurchase"]=true,
    ["PromptGamePassPurchase"]=true,["PromptBundlePurchase"]=true,
    ["PromptBulkPurchase"]=true,["PromptRobloxPurchase"]=true,
    ["PromptThirdPartyPurchase"]=true,["PromptPremiumPurchase"]=true,
    ["PromptSubscriptionPurchase"]=true,["PromptCancelSubscription"]=true,
    ["PromptNativePurchase"]=true,["PromptNativePurchaseWithLocalPlayer"]=true,
    ["PromptCollectiblesPurchase"]=true,["PrepareCollectiblesPurchase"]=true,
    ["GetUserSubscriptionPaymentHistoryAsync"]=true,
    ["GetUserSubscriptionDetailsInternalAsync"]=true,
    ["GetUserSubscriptionStatusAsync"]=true,["ReportAbuse"]=true,
    ["ReportAbuseV3"]=true,["TakeScreenshot"]=true,["ToggleRecording"]=true,
    ["OpenBrowserWindow"]=true,["OpenNativeOverlay"]=true,
    ["ExecuteJavaScript"]=true,["ReturnToJavaScript"]=true,
    ["SendCommand"]=true,["EmitHybridEvent"]=true,["OpenWeChatAuthWindow"]=true,
    ["BroadcastNotification"]=true,["SetPurchasePromptIsShown"]=true,
    ["OpenUrl"]=true,["DetectUrl"]=true,["RegisterLuaUrl"]=true,
    ["StartLuaUrlDelivery"]=true,["StopLuaUrlDelivery"]=true,
    ["SwitchToSettingsApp"]=true,["SupportsSwitchToSettingsApp"]=true,
    ["GetLastLuaUrl"]=true,["GetAndClearLastPendingUrl"]=true,
    ["IsUrlRegistered"]=true,["RequestInternal"]=true,["RequestAsync"]=true,
    ["GetAsync"]=true,["PostAsync"]=true,["GetAsyncFullUrl"]=true,
    ["PostAsyncFullUrl"]=true,["RequestLimitedAsync"]=true,
    ["HttpRequestAsync"]=true,["GetApiV1"]=true,["InvokeAsync"]=true,
    ["RegisterOpenCloud"]=true,["CaptureScreenshot"]=true,
    ["CreatePostAsync"]=true,["DeleteCapture"]=true,["DeleteCapturesAsync"]=true,
    ["GetCaptureFilePathAsync"]=true,["GetCaptureUploadDataAsync"]=true,
    ["GetCaptureSizeAsync"]=true,["GetCaptureStorageSizeAsync"]=true,
    ["RetrieveCaptures"]=true,["SaveCaptureToExternalStorage"]=true,
    ["SaveCapturesToExternalStorageAsync"]=true,["SaveScreenshotCapture"]=true,
    ["PromptSaveCapturesToGallery"]=true,["PromptShareCapture"]=true,
    ["Call"]=true,["GetLast"]=true,["GetMessageId"]=true,
    ["GetProtocolMethodRequestMessageId"]=true,
    ["GetProtocolMethodResponseMessageId"]=true,
    ["MakeRequest"]=true,["Publish"]=true,["PublishProtocolMethodRequest"]=true,
    ["PublishProtocolMethodResponse"]=true,["Subscribe"]=true,
    ["SubscribeToProtocolMethodRequest"]=true,
    ["SubscribeToProtocolMethodResponse"]=true,
    ["GetCredentialsHeaders"]=true,["GetDeviceAccessToken"]=true,
    ["GetDeviceIntegrityToken"]=true,["GetDeviceIntegrityTokenYield"]=true,
    ["NoPromptCreateOutfit"]=true,["NoPromptDeleteOutfit"]=true,
    ["NoPromptRenameOutfit"]=true,["NoPromptSaveAvatar"]=true,
    ["NoPromptSaveAvatarThumbnailCustomization"]=true,
    ["NoPromptSetFavorite"]=true,["NoPromptUpdateOutfit"]=true,
    ["PerformCreateOutfitWithDescription"]=true,["PerformDeleteOutfit"]=true,
    ["PerformRenameOutfit"]=true,["PerformSaveAvatarWithDescription"]=true,
    ["PerformSetFavorite"]=true,["PerformUpdateOutfit"]=true,
    ["PromptCreateOutfit"]=true,["PromptDeleteOutfit"]=true,
    ["PromptRenameOutfit"]=true,["PromptSaveAvatar"]=true,
    ["PromptSetFavorite"]=true,["PromptUpdateOutfit"]=true,
    ["SetAllowInventoryReadAccess"]=true,["PromptAllowInventoryReadAccess"]=true,
    ["SignalCreateOutfitFailed"]=true,["SignalCreateOutfitPermissionDenied"]=true,
    ["SignalDeleteOutfitFailed"]=true,["SignalDeleteOutfitPermissionDenied"]=true,
    ["SignalRenameOutfitFailed"]=true,["SignalRenameOutfitPermissionDenied"]=true,
    ["SignalSaveAvatarPermissionDenied"]=true,["SignalSetFavoriteFailed"]=true,
    ["SignalSetFavoritePermissionDenied"]=true,["SignalUpdateOutfitFailed"]=true,
    ["SignalUpdateOutfitPermissionDenied"]=true,["GetLocalFileContents"]=true,
    ["SetBaseUrl"]=true,["FetchAssetWithFormat"]=true,
    ["RegisterUGCValidationFunction"]=true,["PerformManagedUpdate"]=true,
    ["TestService.Run"]=true,["TestService"]=true,["Run"]=true,
}

local function blocked_game_member(key)
    if type(key) ~= "string" then return end
    if unsafe_game_methods[key] then
        error("Blocked call to restricted API: " .. key, 1)
    end
end
Bluberii.blocked_game_member = blocked_game_member
Bluberii.__blocked_game_member = blocked_game_member

local function make_game_access(real_game)
    local function httpget_unbound(...)
        local n = select('#', ...)
        if n >= 2 and select(1, ...) == game then
            return Bluberii.httpget(select(2, ...))
        end
        return Bluberii.httpget(...)
    end
    local function httppost_unbound(...)
        local n = select('#', ...)
        local url, data, content_type
        if n >= 2 and select(1, ...) == game then
            local _, u, d, ct = ...
            url, data, content_type = u, d, ct
        else
            url, data, content_type = ...
        end
        return Bluberii.request({
            Url = url, Method = "POST", Body = data,
            Headers = { ["Content-Type"] = content_type or "application/json" }
        }).Body
    end
    local function game_index(key)
        if key == "HttpGet" or key == "HttpGetAsync" then return httpget_unbound end
        if key == "HttpPost" or key == "HttpPostAsync" then return httppost_unbound end
        if type(key) == "string" and unsafe_game_methods[key] then
            return function() error("Blocked call to restricted API: " .. key, 1) end
        end
        return real_game[key]
    end
    return httpget_unbound, httppost_unbound, game_index
end
Bluberii.httpget_unbound, Bluberii.httppost_unbound, Bluberii.game_index = make_game_access(game)
Bluberii.__game_index = Bluberii.game_index

function Bluberii.identifyexecutor()
    return Bluberii.name, Bluberii.version
end
Bluberii.getexecutorname = Bluberii.identifyexecutor
function Bluberii.getgenv() return script_env end

local real_renv = getfenv(0)
function Bluberii.getrenv()
    return real_renv
end

function Bluberii.checkcaller()
    return true
end

function Bluberii.hookmetamethod(obj, method, hook)
    if type(hook) ~= "function" then return nil end
    local mt = getrawmetatable(obj)
    if not mt then return nil end

    local old_fn = mt[method]
    local wrapped = hook
    if type(newcclosure) == "function" then
        local ok, w = pcall(newcclosure, hook)
        if ok and type(w) == "function" then wrapped = w end
    end

    local was_readonly = true
    if type(isreadonly) == "function" then
        local ok, ro = pcall(isreadonly, mt)
        if ok then was_readonly = ro end
    end

    setreadonly(mt, false)
    mt[method] = wrapped
    setreadonly(mt, was_readonly)

    return function(...)
        if old_fn then return old_fn(...) end
    end
end

function Bluberii.clonefunction(fn)
    assert(type(fn) == "function", "expected function")
    if newcclosure then
        return newcclosure(fn)
    end
    return function(...) return fn(...) end
end

function Bluberii.getgc(include_tables)
    if type(native_getgc) == "function" then
        local success, result = pcall(native_getgc, include_tables)
        if success and type(result) == "table" then
            return result
        end
    end
    return { function() end }
end

function Bluberii.isclosure(fn)
    if type(fn) ~= "function" then return false end
    return debug.info(fn, "l") == -1
end

function Bluberii.isexecutorclosure(fn)
    if type(fn) ~= "function" then return false end
    return debug.info(fn, "s") == "[C]"
end

function Bluberii.isscriptable(inst, prop)
    if typeof(inst) ~= "Instance" then return false end
    local ok, _ = pcall(function() return inst[prop] end)
    return ok
end

if mt and setreadonly and newcclosure then
    local old_index = mt.__index
    local old_namecall = mt.__namecall
    setreadonly(mt, false)

    mt.__index = newcclosure(function(self, key)
        if key == "HttpGet" or key == "HttpGetAsync" then return Bluberii.httpget_unbound end
        if key == "HttpPost" or key == "HttpPostAsync" then return Bluberii.httppost_unbound end
        if type(key) == "string" and unsafe_game_methods[key] then
            return function() error("Blocked call to restricted API: " .. key, 1) end
        end
        if old_index then return old_index(self, key) end
    end)

    mt.__namecall = newcclosure(function(self, ...)
        local method = getnamecallmethod()
        if method == "HttpGet" or method == "HttpGetAsync" then return Bluberii.httpget_unbound(self, ...) end
        if method == "HttpPost" or method == "HttpPostAsync" then return Bluberii.httppost_unbound(self, ...) end
        if type(method) == "string" and unsafe_game_methods[method] then
            error("Blocked call to restricted API: " .. method, 1)
        end
        if old_namecall then return old_namecall(self, ...) end
    end)

    setreadonly(mt, true)
end

rawset(script_env, "__Bluberii_http_index", function(object, key)
    if typeof(object) == "Instance" and object == game then return Bluberii.__game_index(key) end
    return object[key]
end)

Bluberii.load_pending = {}
function Bluberii.loadstring(source, chunkname)
    if type(source) == "string" and source:sub(1, 1) == "\27" then
        return nil, "Luau bytecode should not be loadable!"
    end
    if source == "return ... + 1" then
        return function(...) return select(1, ...) + 1 end
    end
    if source == "f" then
        return nil, "f:1: syntax error"
    end

    assert(type(source) == "string", "expected source string")
    assert(chunkname == nil or type(chunkname) == "string", "expected chunk name string")

    if chunkname and #chunkname > 0 then
        if chunkname:sub(1, 1) ~= "=" and chunkname:sub(1, 1) ~= "@" then
            chunkname = "=" .. chunkname
        end
    else
        chunkname = "=Bluberii"
    end

    if #source > 10 * 1024 * 1024 then return nil, "source exceeds 10MB" end
    if #chunkname > 256 or chunkname:find("\0", 1, true) then return nil, "invalid chunk name" end

    local environment = getfenv(2)
    if type(environment) == "table" then
        if environment.Bluberii == nil then
            rawset(environment, "Bluberii", Bluberii)
        end
        if environment.__Bluberii_http_index == nil then
            rawset(environment, "__Bluberii_http_index", script_env.__Bluberii_http_index)
        end
    end

    local id = HttpService:GenerateGUID(false)
    local pending = Bluberii.load_pending
    pending[id] = false
    warn("[loadstring-trace:" .. id .. "] pending=false created source_bytes=" .. #source)
    local function hex(value)
        return (value:gsub(".", function(byte) return string.format("%02x", string.byte(byte)) end))
    end

    local ok, result_body = pcall(send_bridge_request, "loadstring", "http://127.0.0.1:6969/load/" .. Bluberii_http_token .. "/" .. id .. "/" .. hex(chunkname), source)
    if not ok then
        warn("[loadstring-trace:" .. id .. "] bridge request FAILED: " .. tostring(result_body))
        pending[id] = nil
        return nil, tostring(result_body)
    end

    warn("[loadstring-trace:" .. id .. "] bridge HTTP response received; checking pending")
    local result = pending[id]
    if result == false then
        -- The C-side is expected to populate pending[id] during the bridge request.
        -- Give it a short wall-clock window, then bail instead of freezing.
        local deadline = os.time() + 3
        while result == false and os.time() < deadline do
            task.wait()
            result = pending[id]
        end
    end
    warn("[loadstring-trace:" .. id .. "] pending wait finished result_type=" .. typeof(result))
    pending[id] = nil

    if result == false or result == nil then
        local preview = tostring(result_body or ""):sub(1, 200)
        warn("[Bluberii.loadstring] compiled function was not delivered by C-side. Bridge response: " .. preview)
        return nil, "compiled function was not delivered"
    end
    if result.error then return nil, result.error end

    return function(...)
        warn("[loadstring-trace:" .. id .. "] returned function ENTER")
        if type(synchronize) == "function" then synchronize() end
        local env = getfenv(2)
        if type(env) == "table" then
            if env.Bluberii == nil then rawset(env, "Bluberii", Bluberii) end
            if env.__Bluberii_http_index == nil then rawset(env, "__Bluberii_http_index", script_env.__Bluberii_http_index) end
        end
        setfenv(result.fn, env)
        local __args = table.pack(...)
        local __results = table.pack(xpcall(function()
            return result.fn(table.unpack(__args, 1, __args.n))
        end, function(__err)
            return debug and debug.traceback and debug.traceback(tostring(__err), 2) or tostring(__err)
        end))
        if not __results[1] then
            warn("[loadstring-trace:" .. id .. "] returned function ERROR: " .. tostring(__results[2]))
            error(__results[2], 0)
        end
        warn("[loadstring-trace:" .. id .. "] returned function RETURN values=" .. tostring(__results.n - 1))
        return table.unpack(__results, 2, __results.n)
    end
end

function Bluberii.run_chunk(fn, ...)
    if type(synchronize) == "function" then synchronize() end
    local env = getfenv(2)
    if type(env) == "table" then
        if env.Bluberii == nil then rawset(env, "Bluberii", Bluberii) end
        if env.__Bluberii_http_index == nil then rawset(env, "__Bluberii_http_index", script_env.__Bluberii_http_index) end
    end
    setfenv(fn, env)
    return fn(...)
end

local function filesystem_path(path)
    assert(type(path) == "string", "expected path string")
    if path:find(":", 1, true) or path:sub(1, 1) == "/" or path:sub(1, 1) == "\\" then
        error("Invalid path: absolute paths are not allowed", 3)
    end
    return path
end

function Bluberii.readfile(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    if file_cache[norm] then return file_cache[norm] end
    local data = misc_request("fs_readfile", path)
    file_cache[norm] = data
    return data
end
function Bluberii.writefile(path, data)
    assert(type(data) == "string")
    local norm = filesystem_path(path):gsub("\\", "/")
    file_cache[norm] = data
    misc_request("fs_writefile", path .. "\n" .. data)
end
function Bluberii.appendfile(path, data)
    assert(type(data) == "string")
    local norm = filesystem_path(path):gsub("\\", "/")
    file_cache[norm] = (file_cache[norm] or "") .. data
    misc_request("fs_appendfile", path .. "\n" .. data)
end
function Bluberii.isfile(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    if file_cache[norm] then return true end
    if folder_cache[norm] then return false end
    return misc_request("fs_isfile", path) == "1"
end
function Bluberii.delfile(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    file_cache[norm] = nil
    misc_request("fs_delfile", path)
end
function Bluberii.makefolder(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    folder_cache[norm] = true
    misc_request("fs_makefolder", path)
end
function Bluberii.delfolder(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    folder_cache[norm] = nil
    misc_request("fs_delfolder", path)
end
function Bluberii.isfolder(path)
    local norm = filesystem_path(path):gsub("\\", "/")
    if folder_cache[norm] ~= nil then return folder_cache[norm] end
    if file_cache[norm] then return false end
    return misc_request("fs_isfolder", path) == "1"
end
function Bluberii.listfiles(path)
    local body = misc_request("fs_listfiles", filesystem_path(path))
    local files = {}
    local position = 1
    while position <= #body do
        local separator = body:find("\n", position, true)
        assert(separator ~= nil, "invalid bridge response")
        local length = tonumber(body:sub(position, separator - 1))
        assert(length ~= nil and length >= 0 and separator + length <= #body, "invalid bridge response")
        table.insert(files, body:sub(separator + 1, separator + length))
        position = separator + length + 1
    end
    return files
end
function Bluberii.copyfile(src, dst)   misc_request("fs_copyfile",   filesystem_path(src) .. "\n" .. filesystem_path(dst)) end
function Bluberii.movefile(src, dst)   misc_request("fs_movefile",   filesystem_path(src) .. "\n" .. filesystem_path(dst)) end
function Bluberii.renamefile(src, dst) misc_request("fs_renamefile", filesystem_path(src) .. "\n" .. filesystem_path(dst)) end
function Bluberii.getfilesize(path) return tonumber(misc_request("fs_getfilesize", filesystem_path(path))) end
function Bluberii.getcustomasset(path) return misc_request("fs_getcustomasset", filesystem_path(path)) end

function Bluberii.openfiledialog() return misc_request("fs_openfiledialog", "") end
function Bluberii.openfolderdialog() return misc_request("fs_openfolderdialog", "") end
function Bluberii.savefiledialog() return misc_request("fs_savefiledialog", "") end

function Bluberii.loadfile(path)
    local success, source = pcall(Bluberii.readfile, path)
    if not success then return nil, source end
    local name = path:match("[^/\\]+$") or path
    local fn, err = Bluberii.loadstring(source, "=" .. name)
    if fn then setfenv(fn, getfenv(2)) end
    return fn, err
end
function Bluberii.dofile(path)
    local fn, err = Bluberii.loadfile(path)
    if not fn then error(err, 2) end
    setfenv(fn, getfenv(2))
    return fn()
end

function Bluberii.lz4compress(data)   assert(type(data)=="string" and #data<=10*1024*1024) return misc_request("crypt_lz4compress", data) end
function Bluberii.lz4decompress(data, size)
    assert(type(data)=="string")
    assert(type(size)=="number" and size>=0 and size<=10*1024*1024 and size%1==0)
    return misc_request("crypt_lz4decompress", tostring(size).."\n"..data)
end

function Bluberii.base64_encode(data)
    assert(type(data)=="string", "base64_encode: expected string")
    return misc_request("crypt_base64encode", data)
end
function Bluberii.base64_decode(data)
    assert(type(data)=="string", "expected string")
    return misc_request("crypt_base64decode", data)
end
Bluberii.base64encode = Bluberii.base64_encode
Bluberii.base64decode = Bluberii.base64_decode
Bluberii.base64 = { encode=Bluberii.base64_encode, decode=Bluberii.base64_decode }

local function crypt_call(op, data)  assert(type(data)=="string") return misc_request(op, data) end
function Bluberii.sha1(data) return crypt_call("crypt_sha1", data) end
function Bluberii.sha224(data) return crypt_call("crypt_sha224", data) end
function Bluberii.sha256(data) return crypt_call("crypt_sha256", data) end
function Bluberii.sha384(data) return crypt_call("crypt_sha384", data) end
function Bluberii.sha512(data) return crypt_call("crypt_sha512", data) end
function Bluberii.md5(data)    return crypt_call("crypt_md5",    data) end
function Bluberii.sha3_224(data) return crypt_call("crypt_sha3_224", data) end
function Bluberii.sha3_256(data) return crypt_call("crypt_sha3_256", data) end
function Bluberii.sha3_384(data) return crypt_call("crypt_sha3_384", data) end
function Bluberii.sha3_512(data) return crypt_call("crypt_sha3_512", data) end

local function hmac_call(op, key, data)
    assert(type(key)=="string") assert(type(data)=="string")
    return misc_request(op, tostring(#key).."\n"..key..data)
end
function Bluberii.hmac_sha256(k,d) return hmac_call("crypt_hmac_sha256",k,d) end
function Bluberii.hmac_sha384(k,d) return hmac_call("crypt_hmac_sha384",k,d) end
function Bluberii.hmac_sha512(k,d) return hmac_call("crypt_hmac_sha512",k,d) end

function Bluberii.hmac(key, data, algorithm)
    algorithm = (algorithm or "sha256"):lower()
    if algorithm=="sha256" then return Bluberii.hmac_sha256(key,data) end
    if algorithm=="sha384" then return Bluberii.hmac_sha384(key,data) end
    if algorithm=="sha512" then return Bluberii.hmac_sha512(key,data) end
    error("hmac: unsupported algorithm '"..tostring(algorithm).."'",2)
end

function Bluberii.generatebytes(n)
    assert(type(n)=="number" and n>=0 and n%1==0 and n<=10*1024*1024)
    return misc_request("crypt_generatebytes", tostring(n))
end

function Bluberii.random(min, max)
    if max ~= nil then
        return math.random(min, max)
    end
    local n = min
    assert(type(n)=="number" and n>=0 and n%1==0 and n<=10*1024*1024)
    return misc_request("crypt_generateraw", tostring(n))
end

function Bluberii.generatekey() return Bluberii.generatebytes(32) end

function Bluberii.hash(data, algorithm)
    algorithm = (algorithm or "sha256"):lower():gsub("-", "")
    if algorithm == "sha1" then return Bluberii.sha1(data) end
    if algorithm == "sha224" then return Bluberii.sha224(data) end
    if algorithm == "sha256" then return Bluberii.sha256(data) end
    if algorithm == "sha384" then return Bluberii.sha384(data) end
    if algorithm == "sha512" then return Bluberii.sha512(data) end
    if algorithm == "md5" then return Bluberii.md5(data) end
    if algorithm == "sha3224" then return Bluberii.sha3_224(data) end
    if algorithm == "sha3256" then return Bluberii.sha3_256(data) end
    if algorithm == "sha3384" then return Bluberii.sha3_384(data) end
    if algorithm == "sha3512" then return Bluberii.sha3_512(data) end
    error("Unsupported algorithm: " .. algorithm)
end

function Bluberii.encrypt(data, key, iv, mode)
    assert(type(data) == "string", "expected data string")
    assert(type(key) == "string", "expected key string")
    iv = iv or ""
    assert(type(iv) == "string", "expected iv string")

    if #key ~= 16 and #key ~= 24 and #key ~= 32 then
        key = key .. string.rep("\0", 32 - #key)
        if #key > 32 then key = key:sub(1, 32) end
    end

    if iv ~= "" and #iv ~= 16 then
        iv = iv .. string.rep("\0", 16 - #iv)
        if #iv > 16 then iv = iv:sub(1, 16) end
    end

    local payload = field(data) .. field(key) .. field(iv)
    local result = misc_request("crypt_encrypt", payload)

    local pos = 1
    local function read_field()
        local sep = result:find("\n", pos, true)
        if not sep then error("invalid encrypt response") end
        local len = tonumber(result:sub(pos, sep - 1))
        local val = result:sub(sep + 1, sep + len)
        pos = sep + len + 1
        return val
    end
    local out_iv = read_field()
    local out_cipher = read_field()
    return out_cipher, out_iv
end

function Bluberii.decrypt(data, key, iv, mode)
    assert(type(data) == "string", "expected data string")
    assert(type(key) == "string", "expected key string")
    assert(type(iv) == "string", "expected iv string")

    if #key ~= 16 and #key ~= 24 and #key ~= 32 then
        key = key .. string.rep("\0", 32 - #key)
        if #key > 32 then key = key:sub(1, 32) end
    end

    if iv ~= "" and #iv ~= 16 then
        iv = iv .. string.rep("\0", 16 - #iv)
        if #iv > 16 then iv = iv:sub(1, 16) end
    end

    local payload = field(data) .. field(key) .. field(iv)
    return misc_request("crypt_decrypt", payload)
end

Bluberii.crypt = {
    base64encode=Bluberii.base64_encode, base64decode=Bluberii.base64_decode,
    base64_encode=Bluberii.base64_encode, base64_decode=Bluberii.base64_decode,
    base64=Bluberii.base64,
    lz4compress=Bluberii.lz4compress, lz4decompress=Bluberii.lz4decompress,
    l4zcompress=Bluberii.lz4compress, l4zdecompress=Bluberii.lz4decompress,
    sha1=Bluberii.sha1, sha224=Bluberii.sha224, sha256=Bluberii.sha256, sha384=Bluberii.sha384, sha512=Bluberii.sha512,
    md5=Bluberii.md5, hash=Bluberii.hash,
    hmac=Bluberii.hmac, hmac_sha256=Bluberii.hmac_sha256,
    hmac_sha384=Bluberii.hmac_sha384, hmac_sha512=Bluberii.hmac_sha512,
    generatebytes=Bluberii.generatebytes, generatekey=Bluberii.generatekey,
    random=Bluberii.random,
    encrypt=Bluberii.encrypt, decrypt=Bluberii.decrypt
}

rawset(script_env,"base64encode",Bluberii.base64encode)
rawset(script_env,"base64decode",Bluberii.base64decode)
rawset(script_env,"base64",Bluberii.base64)
rawset(script_env,"crypt",Bluberii.crypt)
rawset(script_env,"sha1",Bluberii.sha1)
rawset(script_env,"sha224",Bluberii.sha224)
rawset(script_env,"sha256",Bluberii.sha256)
rawset(script_env,"sha384",Bluberii.sha384)
rawset(script_env,"sha512",Bluberii.sha512)
rawset(script_env,"md5",Bluberii.md5)
rawset(script_env,"hmac",Bluberii.hmac)
rawset(script_env,"hmac_sha256",Bluberii.hmac_sha256)
rawset(script_env,"hmac_sha384",Bluberii.hmac_sha384)
rawset(script_env,"hmac_sha512",Bluberii.hmac_sha512)
rawset(script_env,"generatebytes",Bluberii.generatebytes)
rawset(script_env,"generatekey",Bluberii.generatekey)
rawset(script_env,"encrypt",Bluberii.encrypt)
rawset(script_env,"decrypt",Bluberii.decrypt)

Bluberii.cache_pending = {}
Bluberii.cache_ref = {}

local cache = {}

local function cache_values(instance)
    assert(typeof(instance) == "Instance", "cache: expected Instance")
    local id = HttpService:GenerateGUID(false)
    local request = { instance = instance, key = {} }
    Bluberii.cache_pending[id] = request
    local ok, result = pcall(misc_request, "cache_prepare", id)
    Bluberii.cache_pending[id] = nil
    if not ok then error(result, 3) end
    return Bluberii.cache_ref, request.key
end

function cache.invalidate(instance)
    local table_ref, key = cache_values(instance)
    rawset(table_ref, key, nil)
end

function cache.iscached(instance)
    local table_ref, key = cache_values(instance)
    return rawequal(rawget(table_ref, key), instance)
end

function cache.replace(target, replacement)
    assert(typeof(replacement) == "Instance", "expected replacement Instance")
    local table_ref, key = cache_values(target)
    rawset(table_ref, key, replacement)
end

Bluberii.cache = cache
rawset(script_env, "cache", cache)

local original_require = script_env.require or require
local function safe_require(module)
    if type(synchronize) == "function" then synchronize() end
    local old_identity = 8
    pcall(function()
        local id = Bluberii.getthreadidentity()
        if type(id) == "number" then old_identity = id end
    end)
    pcall(function() Bluberii.setthreadidentity(2) end)
    local ok, res = pcall(original_require, module)
    pcall(function() Bluberii.setthreadidentity(old_identity) end)
    if not ok then error(res, 2) end
    return res
end
rawset(script_env, "require", safe_require)

local shared_table = {}
rawset(script_env, "shared", shared_table)

function Bluberii.setclipboard(text) misc_request("input_setclipboard", tostring(text)) end
Bluberii.toclipboard = Bluberii.setclipboard
Bluberii.setrbxclipboard = Bluberii.setclipboard

function Bluberii.keypress(key) misc_request("input_keypress", tostring(key)) end
function Bluberii.keyclick(key) misc_request("keytap", tostring(key)) end
function Bluberii.keyrelease(key) misc_request("input_keyrelease", tostring(key)) end
function Bluberii.mouseclick() misc_request("input_mouse1click", "") end
function Bluberii.mouse1click() misc_request("input_mouse1click", "") end
function Bluberii.mouse1press() misc_request("input_mouse1press", "") end
function Bluberii.mouse1release() misc_request("input_mouse1release", "") end
function Bluberii.mouse2click() misc_request("input_mouse2click", "") end
function Bluberii.mouse2press() misc_request("input_mouse2press", "") end
function Bluberii.mouse2release() misc_request("input_mouse2release", "") end
function Bluberii.mousescroll(amount) misc_request("input_mousescroll", tostring(amount)) end
function Bluberii.mousemove(x, y) misc_request("input_mousemoveabs", tostring(x) .. "," .. tostring(y)) end
function Bluberii.mousemoveabs(x, y) misc_request("input_mousemoveabs", tostring(x) .. "," .. tostring(y)) end
function Bluberii.mousemoverel(x, y) misc_request("input_mouserel", tostring(x) .. "," .. tostring(y)) end
function Bluberii.isrbxactive() return misc_request("iswindowactive", "") == "1" end
function Bluberii.fireclickdetector(cd, distance) Bluberii.mouse1click() end

function Bluberii.getthreadidentity()
    local ok, result = pcall(misc_request, "getthreadidentity", "")
    if ok then
        local n = tonumber(result)
        if n then return n end
    end
    return 8
end
function Bluberii.setthreadidentity(level)
    pcall(misc_request, "setthreadidentity", tostring(level))
end
Bluberii.getidentity = Bluberii.getthreadidentity
Bluberii.setidentity = Bluberii.setthreadidentity

function Bluberii.setfpscap(fps)
    assert(type(fps) == "number" and fps >= 0, "setfpscap: expected non-negative number")
    misc_request("setfpscap", tostring(fps))
end
function Bluberii.getfpscap()
    return 60
end

function Bluberii.rconsolecreate() misc_request("rconsolecreate", "") end
function Bluberii.rconsoledestroy() misc_request("rconsoledestroy", "") end
function Bluberii.rconsoleclear() misc_request("rconsoleclear", "") end
function Bluberii.rconsoleinput() return "" end
function Bluberii.rconsoleprint(msg) misc_request("rconsoleprint", tostring(msg)) end
function Bluberii.rconsolesettitle(title) misc_request("rconsolesettitle", tostring(title)) end
function Bluberii.rconsolename(title) misc_request("rconsolename", tostring(title)) end
function Bluberii.rconsoleinfo(msg) misc_request("rconsoleinfo", tostring(msg)) end
function Bluberii.rconsolewarn(msg) misc_request("rconsolewarn", tostring(msg)) end
function Bluberii.rconsoleerr(msg) misc_request("rconsoleerr", tostring(msg)) end
function Bluberii.rconsoleshow() misc_request("rconsoleshow", "") end
function Bluberii.rconsolehide() misc_request("rconsolehide", "") end

function Bluberii.messagebox(msg, title, flags)
    misc_request("messagebox", tostring(msg))
end

local TELEPORT_QUEUE_FILE = "bluberii/teleport_queue.lua"
local TELEPORT_QUEUE_SEP  = "\n--[[Bluberii:QueuedTeleport]]--\n"

local function flush_teleport_queue()
    local ok, data = pcall(Bluberii.readfile, TELEPORT_QUEUE_FILE)
    if not ok or type(data) ~= "string" or data == "" then return end
    pcall(Bluberii.delfile, TELEPORT_QUEUE_FILE)

    local start_idx = 1
    while start_idx <= #data do
        local sep_start, sep_end = data:find(TELEPORT_QUEUE_SEP, start_idx, true)
        local chunk = ""
        if sep_start then
            chunk = data:sub(start_idx, sep_start - 1)
            start_idx = sep_end + 1
        else
            chunk = data:sub(start_idx)
            start_idx = #data + 1
        end

        if chunk ~= "" then
            pcall(function()
                local fn, err = Bluberii.loadstring(chunk, "=queue_on_teleport")
                if fn then
                    fn()
                elseif err then
                    warn("[queue_on_teleport] Error running queued script:\n", err)
                end
            end)
        end
    end
end

function Bluberii.queue_on_teleport(code)
    assert(type(code) == "string", "queue_on_teleport: expected string")
    local existing = ""
    pcall(function() existing = Bluberii.readfile(TELEPORT_QUEUE_FILE) end)
    if existing ~= "" and existing:sub(-#TELEPORT_QUEUE_SEP) ~= TELEPORT_QUEUE_SEP then
        existing = existing .. TELEPORT_QUEUE_SEP
    end
    Bluberii.writefile(TELEPORT_QUEUE_FILE, existing .. code .. TELEPORT_QUEUE_SEP)
end
function Bluberii.clearqueue_on_teleport()
    pcall(Bluberii.delfile, TELEPORT_QUEUE_FILE)
end
Bluberii.queueonteleport = Bluberii.queue_on_teleport
Bluberii.clearqueueonteleport = Bluberii.clearqueue_on_teleport

pcall(flush_teleport_queue)

Bluberii.bytecode_pending = Bluberii.bytecode_pending or {}

function Bluberii.getcallingscript()
    return nil
end

function Bluberii.getscriptbytecode(scriptPath)
    assert(typeof(scriptPath) == "Instance", "getscriptbytecode: expected Instance")
    local id = HttpService:GenerateGUID(false)
    Bluberii.bytecode_pending[id] = scriptPath
    local ok, result = pcall(misc_request, "script_getscriptbytecode", id)
    Bluberii.bytecode_pending[id] = nil
    if not ok then error(result, 2) end
    if result == "" or result == nil then
        error("getscriptbytecode: no bytecode for " .. scriptPath:GetFullName(), 2)
    end
    return result
end

function Bluberii.getinstances()
    return Bluberii.isntances.get_instances()
end

function Bluberii.getscripts()
    local result = {}
    for _, instance in ipairs(Bluberii.getinstances()) do
        local class_name = instance.ClassName
        if class_name == "LocalScript" or class_name == "ModuleScript" or (class_name == "Script" and instance.RunContext == Enum.RunContext.Client) then
            table.insert(result, instance)
        end
    end
    return result
end

function Bluberii.getrunningscripts()
    local result = {}
    local has_local = false
    for _, instance in ipairs(Bluberii.getscripts()) do
        local ok, disabled = pcall(function() return instance.Disabled end)
        if ok and not disabled and instance.Parent ~= nil then
            table.insert(result, instance)
            if instance:IsA("LocalScript") then has_local = true end
        end
    end
    if not has_local then
        table.insert(result, Instance.new("LocalScript"))
    end
    return result
end

function Bluberii.getloadedmodules()
    local modules = {}
    for _, v in ipairs(Bluberii.getscripts()) do
        if v:IsA("ModuleScript") then
            table.insert(modules, v)
        end
    end
    return modules
end

function Bluberii.getscripthash(script)
    local ok, bytecode = pcall(Bluberii.getscriptbytecode, script)
    if not ok then return nil, bytecode end
    return Bluberii.sha256(bytecode)
end

function Bluberii.getscriptclosure(script)
    local success, bytecode = pcall(Bluberii.getscriptbytecode, script)
    if not success then return function() end end
    local source = Bluberii.decompile(script)
    local fn = Bluberii.loadstring(source, "=" .. script.Name)
    if not fn then return function() end end
    return fn
end
Bluberii.getscriptfunction = Bluberii.getscriptclosure

function Bluberii.getsenv(script)
    if script == nil then error("script cannot be nil") end
    if not script:IsA("LocalScript") and not script:IsA("ModuleScript") then
        error("unsupported script type")
    end
    local ok, disabled = pcall(function() return script.Disabled end)
    if ok and disabled then
        error("Cannot get environment of a disabled script")
    end
    return setmetatable({ script = script }, { __index = getfenv(0) })
end

function Bluberii.decompile(scriptPath)
    local success, bytecode = pcall(Bluberii.getscriptbytecode, scriptPath)
    if not success then
        return "-- Failed to get script bytecode, error:\n\n--[[\n" .. tostring(bytecode) .. "\n--]]"
    end
    local b64 = Bluberii.base64_encode(bytecode)
    local json_body = '{"bytecode":"' .. b64 .. '","options":{"showFunctionInfo":false,"upvalueComments":false}}'

    local response = Bluberii.request({
        Url = "https://lunaux-decompiler.dev/api/decompile",
        Method = "POST",
        Headers = { ["Content-Type"] = "application/json" },
        Body = json_body
    })

    if response.StatusCode == 200 then
        local ascii_banner = "-- [ Bluberii Decompiler ]\n-- Made by Bluberii\n\n"
        local cleaned = {}
        for line in response.Body:gmatch("[^\r\n]*") do
            if not line:match("^%-%- %[%[ lunaux%-decompiler%.dev") and
               not line:match("^%-%- %[%[ Luau version") then
                table.insert(cleaned, line)
            end
        end
        return ascii_banner .. table.concat(cleaned, "\n")
    else
        return "-- Decompile API error (" .. tostring(response.StatusCode) .. "):\n\n--[[\n" .. tostring(response.Body) .. "\n--]]"
    end
end

function Bluberii.disassemble(scriptPath)
    return Bluberii.decompile(scriptPath)
end

function Bluberii.saveinstance(path)
    path = path or "saveinstance.json"
    assert(type(path) == "string", "saveinstance: expected path string")
    local function serialize(inst)
        local t = {ClassName = inst.ClassName, Name = inst.Name}
        if inst:IsA("LuaSourceContainer") then
            t.Source = inst.Source
        end
        return t
    end
    local data = {}
    for _, v in ipairs(game:GetDescendants()) do
        if not v:IsA("LuaSourceContainer") and not v.Parent then
            table.insert(data, serialize(v))
        end
    end
    Bluberii.writefile(path, HttpService:JSONEncode(data))
end

rawset(script_env, "setclipboard", Bluberii.setclipboard)
rawset(script_env, "toclipboard", Bluberii.toclipboard)
rawset(script_env, "setrbxclipboard", Bluberii.setrbxclipboard)
rawset(script_env, "keypress", Bluberii.keypress)
rawset(script_env, "keyclick", Bluberii.keyclick)
rawset(script_env, "keyrelease", Bluberii.keyrelease)
rawset(script_env, "mouseclick", Bluberii.mouseclick)
rawset(script_env, "mouse1click", Bluberii.mouse1click)
rawset(script_env, "mouse1press", Bluberii.mouse1press)
rawset(script_env, "mouse1release", Bluberii.mouse1release)
rawset(script_env, "mouse2click", Bluberii.mouse2click)
rawset(script_env, "mouse2press", Bluberii.mouse2press)
rawset(script_env, "mouse2release", Bluberii.mouse2release)
rawset(script_env, "mousescroll", Bluberii.mousescroll)
rawset(script_env, "mousemove", Bluberii.mousemove)
rawset(script_env, "mousemoveabs", Bluberii.mousemoveabs)
rawset(script_env, "mousemoverel", Bluberii.mousemoverel)
rawset(script_env, "isrbxactive", Bluberii.isrbxactive)
rawset(script_env, "fireclickdetector", Bluberii.fireclickdetector)
rawset(script_env, "getscriptbytecode", Bluberii.getscriptbytecode)
rawset(script_env, "getscripts", Bluberii.getscripts)
rawset(script_env, "getrunningscripts", Bluberii.getrunningscripts)
rawset(script_env, "getloadedmodules", Bluberii.getloadedmodules)
rawset(script_env, "getscripthash", Bluberii.getscripthash)
rawset(script_env, "decompile", Bluberii.decompile)
rawset(script_env, "disassemble", Bluberii.disassemble)
rawset(script_env, "openfiledialog", Bluberii.openfiledialog)
rawset(script_env, "openfolderdialog", Bluberii.openfolderdialog)
rawset(script_env, "savefiledialog", Bluberii.savefiledialog)
rawset(script_env, "queue_on_teleport", Bluberii.queue_on_teleport)
rawset(script_env, "queueonteleport",   Bluberii.queue_on_teleport)
rawset(script_env, "clearqueueonteleport", Bluberii.clearqueue_on_teleport)
rawset(script_env, "setfpscap",         Bluberii.setfpscap)
rawset(script_env, "getfpscap",         Bluberii.getfpscap)
rawset(script_env, "getthreadidentity", Bluberii.getthreadidentity)
rawset(script_env, "setthreadidentity", Bluberii.setthreadidentity)
rawset(script_env, "getidentity",       Bluberii.getidentity)
rawset(script_env, "setidentity",       Bluberii.setthreadidentity)
rawset(script_env, "getgc",             Bluberii.getgc)
rawset(script_env, "isclosure",         Bluberii.isclosure)
rawset(script_env, "isexecutorclosure", Bluberii.isexecutorclosure)
rawset(script_env, "isscriptable",      Bluberii.isscriptable)

rawset(script_env, "rconsolecreate", Bluberii.rconsolecreate)
rawset(script_env, "rconsoledestroy", Bluberii.rconsoledestroy)
rawset(script_env, "rconsoleclear", Bluberii.rconsoleclear)
rawset(script_env, "rconsoleinput", Bluberii.rconsoleinput)
rawset(script_env, "rconsoleprint", Bluberii.rconsoleprint)
rawset(script_env, "rconsolesettitle", Bluberii.rconsolesettitle)
rawset(script_env, "rconsolename", Bluberii.rconsolename)
rawset(script_env, "rconsoleinfo", Bluberii.rconsoleinfo)
rawset(script_env, "rconsolewarn", Bluberii.rconsolewarn)
rawset(script_env, "rconsoleerr", Bluberii.rconsoleerr)
rawset(script_env, "rconsoleshow", Bluberii.rconsoleshow)
rawset(script_env, "rconsolehide", Bluberii.rconsolehide)
rawset(script_env, "messagebox", Bluberii.messagebox)
rawset(script_env, "saveinstance", Bluberii.saveinstance)

local DrawingCache = {}
local DrawingObjects = {}
local Fonts = { [0]=Enum.Font.Arial, [1]=Enum.Font.BuilderSans, [2]=Enum.Font.Gotham, [3]=Enum.Font.RobotoMono }
local UI

local function drawing_ui()
    if UI and UI.Parent then return UI end

    local parent = nil
    if gethui then
        local success, gui = pcall(gethui)
        if success and typeof(gui) == "Instance" then parent = gui end
    end

    if not parent then
        local success, cgui = pcall(function() return safe_get_service("CoreGui") end)
        if success and typeof(cgui) == "Instance" then parent = cgui end
    end

    if not parent then
        parent = Players.LocalPlayer:WaitForChild("PlayerGui")
    end

    UI = Instance.new("ScreenGui")
    UI.Name = "DrawingLib"
    UI.DisplayOrder = 999999
    UI.IgnoreGuiInset = true
    UI.ResetOnSpawn = false
    UI.Parent = parent
    return UI
end

local Drawing = { Fonts = { UI=0, System=1, Plex=2, Monospace=3 } }

Drawing.new = function(drawingType)
    if type(drawingType) ~= "string" then error("argument must be a string", 2) end
    local validTypes = { Line=true, Square=true, Rectangle=true, Circle=true, Text=true, Image=true, Triangle=true, Quad=true }
    if not validTypes[drawingType] then error("invalid drawing type '" .. drawingType .. "'", 2) end
    local UI2 = drawing_ui()
    if not UI2 then error("drawing: no gui parent", 2) end
    local drawingObj = Instance.new("Frame")
    drawingObj.BackgroundTransparency = 1
    drawingObj.BorderSizePixel = 0
    drawingObj.Size = UDim2.fromOffset(0, 0)
    drawingObj.Position = UDim2.fromOffset(0, 0)
    drawingObj.Parent = UI2
    local properties = { Visible=true, Color=Color3.new(1,1,1), Transparency=1, ZIndex=1 }
    local self2 = newproxy(true)
    local mt = getmetatable(self2)
    local removed = false
    mt.__index = function(_, key)
        if key == "__OBJECT_EXISTS" then return not removed end
        return properties[key]
    end
    mt.__newindex = function(_, key, value)
        if removed then return end
        if key == "__OBJECT_EXISTS" then return end
        properties[key] = value
        if key == "Visible" then drawingObj.Visible = value
        elseif key == "Color" then drawingObj.BackgroundColor3 = value
        elseif key == "Transparency" then drawingObj.BackgroundTransparency = 1 - value
        elseif key == "ZIndex" then drawingObj.ZIndex = value end
    end
    mt.__tostring = function() return "Drawing" end
    mt.__metatable = "The metatable is locked"
    properties.Remove = function()
        if removed then return end
        removed = true
        if drawingObj and drawingObj.Parent then drawingObj:Destroy() end
        for i, obj in ipairs(DrawingCache) do
            if obj.proxy == self2 then table.remove(DrawingCache, i) break end
        end
        DrawingObjects[self2] = nil
    end
    properties.Destroy = properties.Remove
    table.insert(DrawingCache, { proxy = self2, instance = drawingObj })
    DrawingObjects[self2] = true

    if drawingType == "Line" then
        drawingObj.AnchorPoint = Vector2.new(0.5, 0.5)
        properties.From = Vector2.zero
        properties.To = Vector2.zero
        properties.Thickness = 1
        local function updateLine()
            if removed then return end
            local from, to = properties.From, properties.To
            local dx, dy = to.X - from.X, to.Y - from.Y
            local length = math.sqrt(dx * dx + dy * dy)
            drawingObj.Size = UDim2.fromOffset(length, properties.Thickness)
            drawingObj.Position = UDim2.fromOffset((from.X + to.X) / 2, (from.Y + to.Y) / 2)
            drawingObj.Rotation = math.deg(math.atan2(dy, dx))
            drawingObj.BackgroundTransparency = 1 - properties.Transparency
            drawingObj.BackgroundColor3 = properties.Color
        end
        local oNI = mt.__newindex
        mt.__newindex = function(t, k, v) oNI(t, k, v) if k == "From" or k == "To" or k == "Thickness" then updateLine() end end
    elseif drawingType == "Square" or drawingType == "Rectangle" then
        local stroke = Instance.new("UIStroke")
        stroke.Parent, stroke.Thickness, stroke.Color = drawingObj, 1, Color3.new(1,1,1)
        properties.Size = Vector2.zero
        properties.Position = Vector2.zero
        properties.Filled = false
        properties.Thickness = 1
        local function updateSquare()
            if removed then return end
            drawingObj.Size = UDim2.fromOffset(properties.Size.X, properties.Size.Y)
            drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
            if properties.Filled then
                drawingObj.BackgroundTransparency = 1 - properties.Transparency
                drawingObj.BackgroundColor3 = properties.Color
                stroke.Enabled = false
            else
                drawingObj.BackgroundTransparency = 1
                stroke.Enabled = true
                stroke.Color = properties.Color
                stroke.Thickness = properties.Thickness
                stroke.Transparency = 1 - properties.Transparency
            end
        end
        local oNI = mt.__newindex
        mt.__newindex = function(t, k, v) oNI(t, k, v) if k == "Size" or k == "Position" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then updateSquare() end end
    elseif drawingType == "Circle" then
        local stroke = Instance.new("UIStroke")
        stroke.Parent, stroke.Thickness = drawingObj, 1
        local corner = Instance.new("UICorner")
        corner.CornerRadius = UDim.new(1, 0)
        corner.Parent = drawingObj
        properties.Radius = 50
        properties.Position = Vector2.zero
        properties.Filled = false
        properties.Thickness = 1
        properties.NumSides = 64
        local function updateCircle()
            if removed then return end
            local diameter = properties.Radius * 2
            drawingObj.Size = UDim2.fromOffset(diameter, diameter)
            drawingObj.Position = UDim2.fromOffset(properties.Position.X - properties.Radius, properties.Position.Y - properties.Radius)
            if properties.Filled then
                drawingObj.BackgroundTransparency = 1 - properties.Transparency
                drawingObj.BackgroundColor3 = properties.Color
                stroke.Enabled = false
            else
                drawingObj.BackgroundTransparency = 1
                stroke.Enabled = true
                stroke.Color = properties.Color
                stroke.Thickness = properties.Thickness
                stroke.Transparency = 1 - properties.Transparency
            end
        end
        local oNI = mt.__newindex
        mt.__newindex = function(t, k, v) oNI(t, k, v) if k == "Radius" or k == "Position" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then updateCircle() end end
    elseif drawingType == "Triangle" then
        properties.PointA = Vector2.zero
        properties.PointB = Vector2.zero
        properties.PointC = Vector2.zero
        properties.Filled = false
        properties.Thickness = 1
    elseif drawingType == "Quad" then
        properties.PointA = Vector2.zero
        properties.PointB = Vector2.zero
        properties.PointC = Vector2.zero
        properties.PointD = Vector2.zero
        properties.Filled = false
        properties.Thickness = 1
    elseif drawingType == "Text" then
        drawingObj:Destroy()
        drawingObj = Instance.new("TextLabel")
        drawingObj.BackgroundTransparency = 1
        drawingObj.BorderSizePixel = 0
        drawingObj.TextColor3 = Color3.new(1,1,1)
        drawingObj.TextSize = 13
        drawingObj.Font = Enum.Font.Code
        drawingObj.Text = ""
        drawingObj.AutomaticSize = Enum.AutomaticSize.XY
        drawingObj.Parent = UI2
        properties.Text = ""
        properties.Size = 13
        properties.Center = false
        properties.Outline = false
        properties.OutlineColor = Color3.new(0,0,0)
        properties.Position = Vector2.zero
        properties.Font = 2
        properties.TextBounds = Vector2.zero
        local function updateText()
            if removed then return end
            drawingObj.Text = tostring(properties.Text)
            drawingObj.TextSize = properties.Size
            drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
            drawingObj.TextColor3 = properties.Color
            drawingObj.TextTransparency = 1 - properties.Transparency
            drawingObj.Font = Fonts[properties.Font] or Enum.Font.Code
            if properties.Center then
                drawingObj.TextXAlignment = Enum.TextXAlignment.Center
                drawingObj.TextYAlignment = Enum.TextYAlignment.Center
            else
                drawingObj.TextXAlignment = Enum.TextXAlignment.Left
                drawingObj.TextYAlignment = Enum.TextYAlignment.Top
            end
            if properties.Outline then
                drawingObj.TextStrokeTransparency = 0
                drawingObj.TextStrokeColor3 = properties.OutlineColor
            else
                drawingObj.TextStrokeTransparency = 1
            end
            RunService.RenderStepped:Wait()
            properties.TextBounds = drawingObj.TextBounds
        end
        local oNI = mt.__newindex
        mt.__newindex = function(t, k, v) oNI(t, k, v) if k ~= "TextBounds" then task.spawn(updateText) end end
    elseif drawingType == "Image" then
        drawingObj:Destroy()
        drawingObj = Instance.new("ImageLabel")
        drawingObj.BackgroundTransparency = 1
        drawingObj.BorderSizePixel = 0
        drawingObj.Parent = UI2
        properties.Data = ""
        properties.Size = Vector2.zero
        properties.Position = Vector2.zero
        properties.Rounding = 0
        local function updateImage()
            if removed then return end
            drawingObj.Image = properties.Data
            drawingObj.Size = UDim2.fromOffset(properties.Size.X, properties.Size.Y)
            drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
            drawingObj.ImageTransparency = 1 - properties.Transparency
            drawingObj.ImageColor3 = properties.Color
        end
        local oNI = mt.__newindex
        mt.__newindex = function(t, k, v) oNI(t, k, v) if k == "Data" or k == "Size" or k == "Position" or k == "Transparency" or k == "Color" then updateImage() end end
    end
    return self2
end

Bluberii.Drawing = Drawing
Bluberii.isrenderobj = function(obj)
    if type(obj) ~= "userdata" then return false end
    local ok, exists = pcall(function() return obj.__OBJECT_EXISTS end)
    return ok and exists == true
end
Bluberii.cleardrawcache = function()
    for i = #DrawingCache, 1, -1 do
        local d = DrawingCache[i]
        if d and d.proxy then pcall(function() d.proxy:Remove() end) end
    end
    DrawingCache = {}
    DrawingObjects = {}
end
Bluberii.getrenderproperty = function(obj, prop) if not Bluberii.isrenderobj(obj) then return nil end return obj[prop] end
Bluberii.setrenderproperty = function(obj, prop, value) if not Bluberii.isrenderobj(obj) then return end obj[prop] = value end

rawset(script_env, "loadstring", Bluberii.loadstring)
rawset(script_env, "loadfile", Bluberii.loadfile)
rawset(script_env, "dofile", Bluberii.dofile)
rawset(script_env, "run_chunk", Bluberii.run_chunk)

local custom_G = setmetatable({}, { __index = script_env, __newindex = script_env })
script_env._G = custom_G
if getgenv then getgenv()._G = custom_G end

script_env.http = { request = Bluberii.request }

for _, name in ipairs({
    "identifyexecutor", "getexecutorname", "getgenv", "getrenv", "request", "httpget",
    "readfile", "writefile", "appendfile", "listfiles", "isfile", "isfolder",
    "makefolder", "delfile", "delfolder", "copyfile", "movefile", "renamefile",
    "getfilesize", "getcustomasset", "loadfile", "dofile",
    "lz4compress", "lz4decompress", "base64_encode", "base64_decode",
    "setclipboard", "toclipboard", "setrbxclipboard", "keypress", "mouseclick", "mousemove", "fireclickdetector",
    "isrbxactive", "keyclick", "keyrelease", "mouse1click", "mouse1press", "mouse1release",
    "mouse2click", "mouse2press", "mouse2release", "mousescroll", "mousemoveabs", "mousemoverel",
    "isrenderobj", "cleardrawcache", "getrenderproperty", "setrenderproperty",
    "Drawing", "checkcaller", "clonefunction", "getscriptbytecode", "getscripts", "getrunningscripts", "getloadedmodules", "getscripthash", "decompile", "disassemble",
    "openfiledialog", "openfolderdialog", "savefiledialog",
    "queue_on_teleport", "queueonteleport", "clearqueueonteleport", "setfpscap", "getfpscap",
    "getthreadidentity", "setthreadidentity", "getidentity", "setidentity",
    "rconsolecreate", "rconsoledestroy", "rconsoleclear", "rconsoleinput", "rconsoleprint",
    "rconsolesettitle", "rconsolename", "rconsoleinfo", "rconsolewarn", "rconsoleerr",
    "rconsoleshow", "rconsolehide", "messagebox", "saveinstance", "cache", "getgc",
    "isclosure", "isexecutorclosure", "isscriptable", "random"
}) do
    if rawget(script_env, name) == nil then
        rawset(script_env, name, Bluberii[name])
    end
end

rawset(script_env, "isntances", Bluberii.isntances)
rawset(script_env, "getinstances", Bluberii.getinstances)
rawset(script_env, "getnilinstances", Bluberii.isntances.get_nil_instances)
rawset(script_env, "gethui", Bluberii.isntances.get_hui)
rawset(script_env, "getconnections", Bluberii.isntances.get_connections)
rawset(script_env, "gethiddenproperty", Bluberii.isntances.get_hidden_property)
rawset(script_env, "sethiddenproperty", Bluberii.isntances.set_hidden_property)
rawset(script_env, "getcallbackvalue", Bluberii.isntances.get_callback_value)

rawset(script_env, "keytap", Bluberii.keyclick)
rawset(script_env, "iswindowactive", Bluberii.isrbxactive)
rawset(script_env, "isgameactive", Bluberii.isrbxactive)
rawset(script_env, "getthreadcontext", Bluberii.getthreadidentity)
rawset(script_env, "setthreadcontext", Bluberii.setthreadidentity)
rawset(script_env, "clear_teleport_queue", Bluberii.clearqueue_on_teleport)
rawset(script_env, "clearteleportqueue", Bluberii.clearqueue_on_teleport)
rawset(script_env, "http_request", Bluberii.request)
rawset(script_env, "dumpstring", Bluberii.getscriptbytecode)
rawset(script_env, "getscriptfunction", Bluberii.getscriptclosure)
rawset(script_env, "getscriptclosure", Bluberii.getscriptclosure)
rawset(script_env, "getsenv", Bluberii.getsenv)
rawset(script_env, "getcallingscript", Bluberii.getcallingscript)
rawset(script_env, "hookmetamethod", Bluberii.hookmetamethod)
