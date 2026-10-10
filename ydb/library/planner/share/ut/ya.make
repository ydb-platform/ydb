UNITTEST()

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/threading/future
    ydb/library/planner/share
)

SRCS(
    shareplanner_ut.cpp
)

END()
