UNITTEST_FOR(ydb/core/yq/libs/control_plane_proxy) 

OWNER(g:yq) 

PEERDIR(
    library/cpp/testing/unittest 
    ydb/core/base 
    ydb/core/testlib 
    ydb/core/yq/libs/actors/logging
    ydb/core/yq/libs/control_plane_storage 
    ydb/core/yq/libs/test_connection
    ydb/library/folder_service 
    ydb/library/folder_service/mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    control_plane_proxy_ut.cpp
)

END()
