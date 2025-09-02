GTEST(unittester-library-global-two_modules)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yt/global
    library/cpp/yt/misc

    library/cpp/yt/global/mock_modules/module1_public
    library/cpp/yt/global/mock_modules/module1_defs

    library/cpp/yt/global/mock_modules/module2_public
    library/cpp/yt/global/mock_modules/module2_defs
)

END()
