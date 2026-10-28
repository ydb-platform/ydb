BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

YQL_UDF_FAT(histogram_udf)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/histogram/ya.make.inc)

END()
