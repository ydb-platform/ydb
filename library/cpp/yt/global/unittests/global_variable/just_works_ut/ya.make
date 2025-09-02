GTEST(unittester-library-global-just-works)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yt/global

    library/cpp/yt/global/mock_modules/module1_public
    library/cpp/yt/global/mock_modules/module1_defs
)

END()
