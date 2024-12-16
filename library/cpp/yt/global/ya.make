LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    impl.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/misc
    library/cpp/yt/memory
    library/cpp/yt/threading
)

END()

RECURSE_FOR_TESTS(
    unittests/global_variable/just_works_ut
    unittests/global_variable/missing_module_ut
    unittests/global_variable/odr_violation_avoidance_ut
    unittests/global_variable/two_modules_ut
)
