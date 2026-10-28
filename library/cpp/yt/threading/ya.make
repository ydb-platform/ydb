LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    execution_stack.cpp
    public.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/cpu_clock
    library/cpp/yt/exception
    library/cpp/yt/system
    library/cpp/yt/memory
)

END()
