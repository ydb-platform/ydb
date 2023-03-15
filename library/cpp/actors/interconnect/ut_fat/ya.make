UNITTEST()

SIZE(LARGE)

TAG(ya:fat)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    library/cpp/actors/interconnect/mock
    library/cpp/actors/interconnect/ut/lib
    library/cpp/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    library/cpp/deprecated/atomic
)

END()
