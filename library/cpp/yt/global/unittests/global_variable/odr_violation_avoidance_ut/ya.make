GTEST(unittester-library-global-ord-violation-avoidence)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yt/global

    library/cpp/yt/global/mock_modules/module3_public
    library/cpp/yt/global/mock_modules/module3_defs
)

END()
