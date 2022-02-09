LIBRARY()

OWNER(g:kikimr)

SRCS(
    stock_workload.cpp
    workload_factory.cpp
)

PEERDIR(
    ydb/public/api/protos
)

END()
