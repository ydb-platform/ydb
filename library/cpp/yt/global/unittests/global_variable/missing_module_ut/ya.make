GTEST(unittester-library-global-missing-module)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yt/global/mock_modules/module1_public
)

END()
