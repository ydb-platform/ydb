UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    kqp_client_data_writes_ut.cpp
    kqp_acl_ut.cpp
    kqp_constraints_ut.cpp
    kqp_generated_columns_ut.cpp
    kqp_scheme_ut.cpp
    kqp_secrets_ut.cpp
    kqp_scheme_fulltext_ut.cpp
    kqp_scheme_index_copy_ut.cpp
    kqp_scheme_type_info_ut.cpp
    kqp_user_management_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/import
    yql/essentials/types/binary_json
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/public/lib/ydb_cli/dump/util
    ydb/library/yql/udfs/common/knn
    ydb/services/workload_manager/ut/common
    ydb/core/tx/columnshard/hooks/testing
    ydb/public/sdk/cpp/src/client/arrow
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(tiering_test_enums.h)

END()
