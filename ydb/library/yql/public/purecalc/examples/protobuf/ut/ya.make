EXECTEST()

RUN(protobuf ${ARCADIA_BUILD_ROOT}/yql/udfs STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    ydb/library/yql/public/purecalc/examples/protobuf
    yql/udfs/common/url
    yql/udfs/common/ip
)

END()
