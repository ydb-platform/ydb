# NO_BUILD_IF does not like logical expressions by now
# see DEVTOOLSSUPPORT-44378
IF (NOT OS_LINUX OR NOT CLANG)
    SET(DISABLE_HYPERSCAN_BUILD)
ENDIF()

NO_BUILD_IF(DISABLE_HYPERSCAN_BUILD)

YQL_UDF_CONTRIB(hyperscan_udf)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/hyperscan/ya.make.inc)

END()
