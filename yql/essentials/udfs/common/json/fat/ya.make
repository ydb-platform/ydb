BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

IF (NOT EXPORT_CMAKE AND SANITIZER_TYPE != "undefined")
    YQL_UDF_FAT(json_udf)
    INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/json/ya.make.inc)
    END()

ENDIF()
