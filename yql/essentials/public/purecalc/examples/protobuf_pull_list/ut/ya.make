EXECTEST()

RUN(protobuf_pull_list STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    yql/essentials/public/purecalc/examples/protobuf_pull_list
)

END()
