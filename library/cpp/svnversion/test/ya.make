PROGRAM()
PEERDIR(library/cpp/svnversion)
SRCS(main.cpp)

# Speed up building times - target is used in integration tests
ALLOCATOR(B)

END()
