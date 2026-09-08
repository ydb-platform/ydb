PRAGMA UdfBridge;
-- XXX: The UDF crash kills the bridge worker. Graph cleanup must
-- preserve that error while releasing bridge proxies to the dead
-- worker, rather than aborting. LLVM skips its generated callable
-- release after an exception, hiding this cleanup path.
PRAGMA config.flags("LLVM_OFF");

SELECT SimpleUdf::Crash("boom");
