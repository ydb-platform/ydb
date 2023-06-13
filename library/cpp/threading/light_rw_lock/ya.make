LIBRARY()

SRCS(
    lightrwlock.cpp
    lightrwlock.h
)

END()

RECURSE(
    bench
    ut
)
