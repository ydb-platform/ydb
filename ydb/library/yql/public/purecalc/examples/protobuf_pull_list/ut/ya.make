EXECTEST()

RUN(protobuf_pull_list STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    ydb/library/yql/public/purecalc/examples/protobuf_pull_list
)

END()
