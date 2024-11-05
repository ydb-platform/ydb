LIBRARY()

PEERDIR(
    ydb/library/yql/public/purecalc/common
)

INCLUDE(ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
