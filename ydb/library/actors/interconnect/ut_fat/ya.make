UNITTEST()

SIZE(LARGE)

TAG(ya:fat)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/mock
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    library/cpp/deprecated/atomic
)

END()
