local skynet = require "skynet"
local mongo = require "skynet.db.mongo"
local bson = require "bson"

local host, port, db_name, tbl, username, password, certfile, keyfile = ...
if port then
	port = math.tointeger(port)
end

math.randomseed(os.time()) -- 设置随机数种子，以确保每次运行程序时得到不同的结果
 
function genRandomString(length)
    local characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    local result = ""
    for i = 1, length do
        local index = math.random(1, #characters)
        result = result .. string.sub(characters, index, index)
    end
    return result
end

local function _create_tls_client()
	return mongo.client({
		host = host, port = port,
		username = username, password = password,
		authdb = "admin",
		tls = true,
		tlsClientCert = certfile,
		tlsClientKey = keyfile
	})
end

local function test_tls_query()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]
	-- db.mytbl:safe_insert({name = "Alice", age = 25})
	-- db.mytbl:safe_insert({name = "Bob", age = 30})
	-- db.mytbl:safe_insert({name = "Charlie", age = 35})
	-- db.mytbl:safe_insert({name = "David", age = 40})
	-- db.mytbl:safe_insert({name = "Joe", age = 19})
	-- db.mytbl:safe_insert({name = "Jim", age = 19})
	-- db.mytbl:safe_insert({name = "Jim", age = 19})
	-- ret = db.mydoc:findOne({first_name = "Olivia"})
	-- print("query result:", ret.first_name, ret.last_name, ret.city, ret.address)
	local cursor = db.mydoc:find()
	while cursor:hasNext() do
		ret = cursor:next()
		print("query result:", ret.first_name, ret.last_name, ret.city, ret.address)
	end
end

local function test_tls_bench()
	local ok, err, ret, ts, dT
	local c = _create_tls_client()
	local db = c[db_name]
	ts = os.time()
	local n = 10000
	for i=1,n do
		local name = genRandomString(4)
		db[tbl]:safe_insert({name = name, age = i%n})
	end
	local dT = os.time() - ts
	print(string.format("insert %d take seconds: %d", n, dT))

	ts = os.time()
	for i=1,n/10 do
		ret = db[tbl]:findOne({age = i})
		-- print(string.format("query result: name %s age %d", ret.name, ret.age))
	end
	dT = os.time() - ts
	print(string.format("query %d take seconds: %d", n/10, dT))
end

skynet.start(function()
	print("=========== bench start ===========")
	test_tls_bench()
	print("=========== bench finish ===========")
end)
