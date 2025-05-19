PROGRAM()

PEERDIR(
    ydb/library/actors/interconnect/rdma
    ydb/library/actors/interconnect/rdma/ibdrv
)

SRCS(
    main.cpp
    rdma.cpp
    sock.cpp
)

END()
