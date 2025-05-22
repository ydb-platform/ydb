LIBRARY()

PEERDIR(
    yql/essentials/public/purecalc/common
)

INCLUDE(ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
