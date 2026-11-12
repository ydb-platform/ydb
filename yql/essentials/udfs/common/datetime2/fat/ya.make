BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

YQL_UDF_FAT(datetime2_udf)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/datetime2/ya.make.inc)

END()
