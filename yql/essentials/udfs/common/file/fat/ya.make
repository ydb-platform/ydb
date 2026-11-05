BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

YQL_UDF_FAT(file_udf)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/file/ya.make.inc)

END()
