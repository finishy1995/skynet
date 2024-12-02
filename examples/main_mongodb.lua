local skynet = require "skynet"


skynet.start(function()
	print("Main Server start")
	local console = skynet.newservice(
		"testmongodb", "test-shard-00-02.phc4r.mongodb.net", 27017, "mydb", "mytbl", "test", "test2024SSS", "/home/joe/client.crt", "/home/joe/client.key"
		-- "testmongodb", "127.0.0.1", 27017, "mydb", "mydoc", "root", "123456", "/home/joe/soft/mongodb/cert/client.crt", "/home/joe/soft/mongodb/cert/client.key"
	)
	
	print("Main Server exit")
	skynet.exit()
end)
