UNITTEST_FOR(ydb/library/yql/core/file_storage) 

OWNER(g:yql)

IF (OPENSOURCE) 
    CFLAGS( 
        -DOPENSOURCE 
    ) 
ENDIF() 
 
 
SRCS(
    file_storage_ut.cpp
    sized_cache_ut.cpp
    storage_ut.cpp
    test_http_server.cpp
    url_mapper_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/threading/future
    ydb/library/yql/core/file_storage/http_download
)

END()
