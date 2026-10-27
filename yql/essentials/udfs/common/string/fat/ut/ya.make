BUILD_ONLY_IF(OS_LINUX)

UNITTEST()

YQL_CURRENT_ABI_VERSION()

CFLAGS(-DYQL_UDF_STATIC_ENTRY_POINT=GetStaticUdfLibrary_string_udf)

BUNDLE_OUTPUT(yql/essentials/udfs/common/string/fat string_udf_internalized.a RENAME cached.a)

SET_APPEND(LDFLAGS ${input:"cached.a"})

SRCS(
    static_udf_ut.cpp
    GLOBAL yql/essentials/public/udf/udf_cached_registrator.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/math/impl
)

END()
