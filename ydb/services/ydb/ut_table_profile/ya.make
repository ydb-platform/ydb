UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(12)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_ut_table_profile.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/pg
    ydb/core/tx/datashard/ut_common
    ydb/core/grpc_services/base
    ydb/core/testlib
    ydb/core/security
    ydb/core/security/ldap_auth_provider/test_utils
    yql/essentials/minikql/dom
    yql/essentials/minikql/jsonpath
    ydb/library/testlib/service_mocks/ldap_mock
    ydb/public/lib/experimental
    ydb/public/lib/json_value
    ydb/public/lib/yson_value
    ydb/public/lib/ut_helpers
    ydb/public/lib/ydb_cli/commands
    ydb/core/kqp/common/events
    ydb/core/kqp/common/shutdown
    ydb/core/kqp/common/simple
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/extension_common
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/monitoring
    ydb/services/ydb
    ydb/services/ydb/ut_common
)

YQL_LAST_ABI_VERSION()

END()
