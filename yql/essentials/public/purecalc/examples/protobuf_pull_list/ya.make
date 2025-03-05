PROGRAM()

SRCS(
    main.proto
    main.cpp
)

PEERDIR(
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/io_specs/protobuf
    yql/essentials/public/purecalc/helpers/stream
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
