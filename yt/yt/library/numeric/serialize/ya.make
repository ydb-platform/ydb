LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/library/numeric
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
    
