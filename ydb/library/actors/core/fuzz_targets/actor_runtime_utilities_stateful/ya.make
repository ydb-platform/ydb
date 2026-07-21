FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
    library/cpp/threading/chunk_queue
    ydb/library/actors/core
    ydb/library/actors/util
)

END()
