local skynet = require "skynet"
local mongo = require "skynet.db.mongo"
local bson = require "bson"

local mongo_pool = {}
local pool_meta = {
    __index = mongo_pool,
}

local default_pool_size = 10

-- 创建一个新的池
function mongo_pool.new(config)
    local self = {
        hosts = config.hosts or {}, -- 主机列表 { {host="ip1", port=27017}, ... }
        pool_size = config.pool_size or default_pool_size, -- 每个主机创建的客户端数量
        username = config.username or "",
        password = config.password or "",
        tls = config.tls or false,
        tlsClientCert = config.tlsClientCert or "",
        tlsClientKey = config.tlsClientKey or "",
        available_clients = {}, -- 可用客户端池
        in_use_clients = {}, -- 正在使用的客户端
        wait_queue = {}, -- 等待队列
    }
    setmetatable(self, pool_meta)
    self:initialize()
    return self
end

-- 初始化池
function mongo_pool:initialize()
    for _, host_config in ipairs(self.hosts) do
        local host = host_config.host
        local port = host_config.port or 27017

        -- 为每个主机创建 n 个客户端
        for i = 1, self.pool_size do
            local client_config = {
                host = host,
                port = port,
                username = self.username,
                password = self.password,
                authdb = "admin",
                tls = self.tls,
                tlsClientCert = self.tlsClientCert,
                tlsClientKey = self.tlsClientKey,
            }
            local client = mongo.client(client_config)

            -- 将客户端添加到可用池
            table.insert(self.available_clients, client)
        end
    end
end

-- 获取一个客户端
function mongo_pool:get_client()
    while true do
        if #self.available_clients > 0 then
            local client = table.remove(self.available_clients, 1)
            self.in_use_clients[client] = true
            return client
        end

        local co = coroutine.running()
        table.insert(self.wait_queue, co)
        skynet.wait(co)
    end
end

-- 归还客户端
function mongo_pool:return_client(client)
    if self.in_use_clients[client] then
        self.in_use_clients[client] = nil
        table.insert(self.available_clients, client)

        if #self.wait_queue > 0 then
            local co = table.remove(self.wait_queue, 1)
            skynet.wakeup(co)
        end
    else
        error("Attempt to return a client that is not in use")
    end
end

-- 关闭所有客户端
function mongo_pool:close()
    for _, client in ipairs(self.available_clients) do
        client:disconnect()
    end

    for client, _ in pairs(self.in_use_clients) do
        client:disconnect()
    end

    self.available_clients = {}
    self.in_use_clients = {}
    self.wait_queue = {}
end

-- 配置项
local host, port, db_name, tbl, username, password, certfile, keyfile = ...
if port then
    port = math.tointeger(port)
end

-- 全局阈值
local THRESHOLD = 1000000
local current_index = 0 -- 当前索引

math.randomseed(os.time())

local function _create_tls_client()
    return mongo_pool.new({
        hosts = {
            {host = host, port = port},
        },
        pool_size = 5,
        username = username,
        password = password,
        authdb = "admin",
        tls = true,
        tlsClientCert = certfile,
        tlsClientKey = keyfile,
    })
end

-- 插入操作
local function insert_task(client, db_name)
    local start_time = skynet.time()
    local c = client:get_client()
    local db = c[db_name]

    current_index = current_index + 1
    local data = {name = "David" .. current_index, id = current_index}

    local success, err = pcall(function()
        db[tbl]:safe_insert(data)
    end)

    local end_time = skynet.time()
    local elapsed_time_ms = (end_time - start_time) * 1000
    client:return_client(c)

    if success then
        print(string.format("Insert task succeeded, execution time: %.2f ms", elapsed_time_ms))
    else
        print(string.format("Insert task failed: %s, error: %s", bson.encode(data), err))
    end
end

-- 更新操作
local function update_task(client, db_name)
    local start_time = skynet.time()
    local c = client:get_client()
    local db = c[db_name]

    local random_id = math.random(0, current_index)
    local update_data = {["$set"] = {name = "UpdatedDavid" .. random_id}}

    local success, err = pcall(function()
        db[tbl]:safe_update({id = random_id}, update_data)
    end)

    local end_time = skynet.time()
    local elapsed_time_ms = (end_time - start_time) * 1000
    client:return_client(c)

    if success then
        print(string.format("Update task succeeded for id %d, execution time: %.2f ms", random_id, elapsed_time_ms))
    else
        print(string.format("Update task failed for id %d, error: %s", random_id, err))
    end
end

-- 删除操作
local function delete_task(client, db_name)
    local start_time = skynet.time()
    local c = client:get_client()
    local db = c[db_name]

    local success, err = pcall(function()
        db[tbl]:safe_delete({id = current_index})
    end)
    current_index = current_index - 1

    local end_time = skynet.time()
    local elapsed_time_ms = (end_time - start_time) * 1000
    client:return_client(c)

    if success then
        print(string.format("Delete task succeeded for id %d, execution time: %.2f ms", current_index, elapsed_time_ms))
    else
        print(string.format("Delete task failed for id %d, error: %s", current_index, err))
    end
end

-- 查询操作
local function query_task(client, db_name)
    local start_time = skynet.time()
    local c = client:get_client()
    local db = c[db_name]

    -- 计算查询条件
    local query_id = math.floor(current_index / 2) -- id = index / 2，向下取整
    local success, err = pcall(function()
        local result = db[tbl]:findOne({id = query_id}) -- 查找 id = query_id 的记录
    end)

    local end_time = skynet.time()
    local elapsed_time_ms = (end_time - start_time) * 1000
    client:return_client(c)

    if success then
        print(string.format("Query task succeeded, execution time: %.2f ms", elapsed_time_ms))
    else
        print(string.format("Query task failed, error: %s", err))
    end
end

-- 测试任务调度
local function test_concurrent_operations(concurrent_count)
    local client = _create_tls_client()

    local function worker(thread_id)
        while true do
            local task_type
            if current_index < THRESHOLD then
                local rand = math.random(1, 10)
                if rand <= 3 then
                    task_type = "insert"
                elseif rand <= 4 then
                    task_type = "update"
                elseif rand <= 5 then
                    task_type = "delete"
                else
                    task_type = "query"
                end
            else
                local rand = math.random(1, 10)
                if rand <= 1 then
                    task_type = "insert"
                elseif rand <= 4 then
                    task_type = "update"
                elseif rand <= 5 then
                    task_type = "delete"
                else
                    task_type = "query"
                end
            end

            if task_type == "insert" then
                insert_task(client, db_name)
            elseif task_type == "update" then
                update_task(client, db_name)
            elseif task_type == "delete" then
                delete_task(client, db_name)
            else
                query_task(client, db_name)
            end
        end
    end

    -- 启动指定数量的线程
    for thread_id = 1, concurrent_count do
        skynet.fork(function()
            worker(thread_id)
        end)
    end
end

skynet.start(function()
    print("=========== bench start ===========")
    local concurrent_count = 10
    test_concurrent_operations(concurrent_count)
    print("=========== bench finish ===========")
end)