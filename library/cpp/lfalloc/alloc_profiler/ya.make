LIBRARY()

OWNER(g:rtmr g:kikimr)

SRCS(
    profiler.cpp
    stackcollect.cpp
)

PEERDIR(
    library/cpp/lfalloc/dbg_info 
    library/cpp/cache 
)

END()

RECURSE(ut)
