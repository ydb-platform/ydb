LIBRARY()

PEERDIR(
    library/cpp/deprecated/accessors
)

SRCS(
    bitinput.cpp
    bitinput_impl.cpp
    bitoutput.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
