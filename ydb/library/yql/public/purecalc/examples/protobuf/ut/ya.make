IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE)

EXECTEST()

RUN(protobuf ${ARCADIA_BUILD_ROOT}/ydb/library/yql/udfs STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    ydb/library/yql/public/purecalc/examples/protobuf
    ydb/library/yql/udfs/common/url_base
    ydb/library/yql/udfs/common/ip_base
)

END()

ENDIF()
