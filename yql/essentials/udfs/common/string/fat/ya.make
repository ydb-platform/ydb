BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

YQL_UDF_FAT(string_udf)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/string/ya.make.inc)

END()

RECURSE_FOR_TESTS(ut)
