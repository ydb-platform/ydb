LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRC(
    test_variable.cpp
)

PEERDIR(
    library/cpp/yt/misc

    library/cpp/yt/global
    library/cpp/yt/global/mock_modules/module2_public
)

END()
