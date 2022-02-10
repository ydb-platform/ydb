UNITTEST_FOR(library/cpp/tvmauth/client) 
 
OWNER(g:passport_infra) 
 
DATA(arcadia/library/cpp/tvmauth/client/ut/files) 
 
PEERDIR( 
    library/cpp/cgiparam
    library/cpp/testing/mock_server 
) 
 
SRCS( 
    async_updater_ut.cpp 
    checker_ut.cpp 
    client_status_ut.cpp 
    default_uid_checker_ut.cpp 
    disk_cache_ut.cpp 
    exponential_backoff_ut.cpp 
    facade_ut.cpp 
    last_error_ut.cpp 
    logger_ut.cpp 
    roles/decoder_ut.cpp 
    roles/entities_index_ut.cpp 
    roles/parser_ut.cpp 
    roles/roles_ut.cpp 
    roles/tvmapi_roles_fetcher_ut.cpp 
    settings_ut.cpp 
    src_checker_ut.cpp 
    tvmapi_updater_ut.cpp 
    tvmtool_updater_ut.cpp 
    utils_ut.cpp 
) 
 
REQUIREMENTS(ram:11)

END() 
