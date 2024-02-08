LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

# Built with ya make -DNO_DEBUGINFO=yes -r --musl contrib/libs/llvm12/tools/llvm-symbolizer
FROM_SANDBOX(
    FILE 2531143113
    OUT_NOAUTO llvm-symbolizer
)

RESOURCE(
    yt/yt/library/ytprof/bundle/llvm-symbolizer
    /ytprof/llvm-symbolizer
)

# Built with env CGO_ENABLED=0 ya tool go install github.com/google/pprof@latest
FROM_SANDBOX(
    FILE 2531135322
    OUT_NOAUTO pprof
)

RESOURCE(
    yt/yt/library/ytprof/bundle/pprof
    /ytprof/pprof
)

END()

