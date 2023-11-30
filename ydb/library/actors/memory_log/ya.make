LIBRARY()

SRCS(
    memlog.cpp
    memlog.h
    mmap.cpp
)

PEERDIR(
    library/cpp/threading/queue
    contrib/libs/linuxvdso
    library/cpp/deprecated/atomic
)

END()
