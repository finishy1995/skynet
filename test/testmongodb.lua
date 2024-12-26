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

-- skynet.start(function()
-- 	print("=========== bench start ===========")
-- 	test_tls_query()
-- 	print("=========== bench finish ===========")
-- end)


local function test_auth()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]
	db:auth(username, password)

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	ok, err, ret = db.testcoll:safe_insert({test_key = 1});
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 1});
	assert(ok and ret and ret.n == 1, err)
end

local function test_insert_without_index()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	ok, err, ret = db.testcoll:safe_insert({test_key = 1});
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 1});
	assert(ok and ret and ret.n == 1, err)
end

local function test_insert_with_index()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	db.testcoll:ensureIndex({test_key = 1}, {unique = true, name = "test_key_index"})

	ok, err, ret = db.testcoll:safe_insert({test_key = 1})
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 1})
	assert(ok == false and string.find(err, "duplicate key error"))
end

local function test_find_and_remove()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	local cursor = db.testcoll:find()
	assert(cursor:hasNext() == false)

	db.testcoll:ensureIndex({test_key = 1}, {test_key2 = -1}, {unique = true, name = "test_index"})

	ok, err, ret = db.testcoll:safe_insert({test_key = 1, test_key2 = 1})
	assert(ok and ret and ret.n == 1, err)

	cursor = db.testcoll:find()
	assert(cursor:hasNext() == true)
	local v = cursor:next()
	assert(v)
	assert(v.test_key == 1)

	ok, err, ret = db.testcoll:safe_insert({test_key = 1, test_key2 = 2})
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 2, test_key2 = 3})
	assert(ok and ret and ret.n == 1, err)

	ret = db.testcoll:findOne({test_key2 = 1})
	assert(ret and ret.test_key2 == 1, err)

	ret = db.testcoll:find({test_key2 = {['$gt'] = 0}}):sort({test_key = 1}, {test_key2 = -1}):skip(1):limit(1)
	assert(ret:count() == 3)
	assert(ret:count(true) == 1)
	if ret:hasNext() then
		ret = ret:next()
	end
	assert(ret and ret.test_key2 == 1)

	db.testcoll:delete({test_key = 1})
	db.testcoll:delete({test_key = 2})

	ret = db.testcoll:findOne({test_key = 1})
	assert(ret == nil)
end

local function test_runcommand()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	ok, err, ret = db.testcoll:safe_insert({test_key = 1, test_key2 = 1})
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 1, test_key2 = 2})
	assert(ok and ret and ret.n == 1, err)

	ok, err, ret = db.testcoll:safe_insert({test_key = 2, test_key2 = 3})
	assert(ok and ret and ret.n == 1, err)

	local pipeline = {
		{
			["$group"] = {
				_id = mongo.null,
				test_key_total = { ["$sum"] = "$test_key"},
				test_key2_total = { ["$sum"] = "$test_key2" },
			}
		}
	}
	ret = db:runCommand("aggregate", "testcoll", "pipeline", pipeline, "cursor", {})
	assert(ret and ret.cursor.firstBatch[1].test_key_total == 4)
	assert(ret and ret.cursor.firstBatch[1].test_key2_total == 6)
end

local function test_expire_index()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:dropIndex("*")
	db.testcoll:drop()

	db.testcoll:ensureIndex({test_key = 1}, {unique = true, name = "test_key_index", expireAfterSeconds = 1, })
	db.testcoll:ensureIndex({test_date = 1}, {expireAfterSeconds = 1, })

	ok, err, ret = db.testcoll:safe_insert({test_key = 1, test_date = bson.date(os.time())})
	assert(ok and ret and ret.n == 1, err)

	ret = db.testcoll:findOne({test_key = 1})
	assert(ret and ret.test_key == 1)

	for i = 1, 60 do
		skynet.sleep(100);
		print("check expire", i)
		ret = db.testcoll:findOne({test_key = 1})
		if ret == nil then
			return
		end
	end
	print("test expire index failed")
	assert(false, "test expire index failed");
end

local function test_safe_batch_insert()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:drop()
	
	local docs, length = {}, 10
	for i = 1, length do
		table.insert(docs, {test_key = i})
	end
	
	db.testcoll:safe_batch_insert(docs)

	local ret = db.testcoll:find()
	assert(length == ret:count(), "test safe batch insert failed")
end

local function test_safe_batch_delete()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:drop()

	local docs, length = {}, 10
	for i = 1, length do
		table.insert(docs, {test_key = i})
	end

	db.testcoll:safe_batch_insert(docs)

	docs = {}
	local del_num = 5
	for i = 1, del_num do
		table.insert(docs, {test_key = i})
	end

	db.testcoll:safe_batch_delete(docs)

	local ret = db.testcoll:find()
	assert((length - del_num) == ret:count(), "test safe batch delete failed")
end

local function test_safe_update()
	local ok, err, ret
	local c = _create_tls_client()
	local db = c[db_name]

	db.testcoll:drop()

	db.testcoll:safe_insert({test_key = 100, test_value = "hello mongo"})
	
	db.testcoll:ensureIndex({test_key = 1}, {unique = true, name = "test_key_index"})

	local query = {test_key = 100}
	local update = {test_value = "hi mongo"}
	ok, err = db.testcoll:safe_update(query, {['$set'] = update})
	assert(ok, err)

	ret = db.testcoll:findOne(query)
	assert(ret.test_value == "hi mongo")
end

skynet.start(function()
	if username then
		print("Test auth")
		test_auth()
	end
	print("Test insert without index")
	test_insert_without_index()
	print("Test insert index")
	test_insert_with_index()
	print("Test find and remove")
	test_find_and_remove()
	print("Test runCommand")
	test_runcommand()
	print("Test expire index")
	test_expire_index()
	print("test safe batch insert")
	test_safe_batch_insert()
	print("test safe batch delete")
	test_safe_batch_delete()
	print("test_safe_update")
	test_safe_update()
	print("mongodb test finish.");
end)