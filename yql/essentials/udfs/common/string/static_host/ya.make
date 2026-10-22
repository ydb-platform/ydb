BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

PROGRAM(string_udf_static_host)

YQL_CURRENT_ABI_VERSION()

IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
    CFLAGS(-DYQL_UDF_STATIC_ENTRY_POINT=GetStaticUdfLibrary_string_udf)
ENDIF()

SRCS(main.cpp)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/string
)

END()
