LIBRARY()

SRCS(
    actors/read_actor.cpp
    actors/memory_quota.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    library/cpp/threading/cancellation
    yql/essentials/minikql/computation
    yql/essentials/public/udf/arrow
)

YQL_LAST_ABI_VERSION()
END()

RECURSE_FOR_TESTS(ut)
