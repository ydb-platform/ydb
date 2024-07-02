UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(60)
IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_bulk_upsert_ut.cpp
    ydb_bulk_upsert_olap_ut.cpp
    ydb_coordination_ut.cpp
    ydb_index_table_ut.cpp
    ydb_import_ut.cpp
    ydb_ut.cpp
    ydb_register_node_ut.cpp
    ydb_scripting_ut.cpp
    ydb_table_ut.cpp
    ydb_stats_ut.cpp
    ydb_logstore_ut.cpp
    ydb_olapstore_ut.cpp
    ydb_monitoring_ut.cpp
    ydb_query_ut.cpp
    ydb_ldap_login_ut.cpp
    ydb_object_storage_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    ydb/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx/datashard/ut_common
    ydb/core/grpc_services/base
    ydb/core/testlib
    ydb/core/security
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/jsonpath
    ydb/library/testlib/service_mocks/ldap_mock
    ydb/public/lib/experimental
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/lib/ut_helpers
    ydb/public/lib/ydb_cli/commands
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_extension
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_monitoring
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:14)

END()
