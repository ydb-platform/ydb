SELECT Core::Decode("key1",AsList("key1","value1","key2","value2"),"default");
SELECT Core::Decode("key2",AsList("key1","value1","key2","value2"),"default");
SELECT Core::Decode("keyZ",AsList("key1","value1","key2","value2"),"default");

