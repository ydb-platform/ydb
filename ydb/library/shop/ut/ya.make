UNITTEST()

PEERDIR(
    library/cpp/threading/future
    ydb/library/shop
)

SRCS(
    estimator_ut.cpp
    flowctl_ut.cpp
    scheduler_ut.cpp
    lazy_scheduler_ut.cpp
)

END()
