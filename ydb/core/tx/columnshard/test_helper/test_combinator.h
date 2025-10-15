#pragma once


#define Y_UNIT_TEST_COMBINATOR_1(BaseName, Flag1)                                                                                  \
    template<bool> void BaseName(NUnitTest::TTestContext&);                                                                        \
    struct TTestRegistration##BaseName {                                                                                           \
        TTestRegistration##BaseName() {                                                                                            \
            TCurrentTest::AddTest(#BaseName "-" #Flag1, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true>), false);  \
        }                                                                                                                          \
    };                                                                                                                             \
    static TTestRegistration##BaseName testRegistration##BaseName;                                                                 \
    template<bool Flag1>                                                                                                           \
    void BaseName(NUnitTest::TTestContext&)

#define Y_UNIT_TEST_COMBINATOR_2(BaseName, Flag1, Flag2)                                                                                             \
    template<bool, bool> void BaseName(NUnitTest::TTestContext&);                                                                                    \
    struct TTestRegistration##BaseName {                                                                                                             \
        TTestRegistration##BaseName() {                                                                                                              \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true>),  false); \
        }                                                                                                                                            \
    };                                                                                                                                               \
    static TTestRegistration##BaseName testRegistration##BaseName;                                                                                   \
    template<bool Flag1, bool Flag2>                                                                                                                 \
    void BaseName(NUnitTest::TTestContext&)


#define Y_UNIT_TEST_COMBINATOR_3(BaseName, Flag1, Flag2, Flag3)                                                                                                        \
    template<bool, bool, bool> void BaseName(NUnitTest::TTestContext&);                                                                                                \
    struct TTestRegistration##BaseName {                                                                                                                               \
        TTestRegistration##BaseName() {                                                                                                                                \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "-" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "-" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "-" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "-" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "+" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "+" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "+" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "+" #Flag3, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  true>),  false); \
        }                                                                                                                                                              \
    };                                                                                                                                                                 \
    static TTestRegistration##BaseName testRegistration##BaseName;                                                                                                     \
    template<bool Flag1, bool Flag2, bool Flag3>                                                                                                                       \
    void BaseName(NUnitTest::TTestContext&)

#define Y_UNIT_TEST_COMBINATOR_4(BaseName, Flag1, Flag2, Flag3, Flag4)                                                                                                                   \
    template<bool, bool, bool, bool> void BaseName(NUnitTest::TTestContext&);                                                                                                            \
    struct TTestRegistration##BaseName {                                                                                                                                                 \
        TTestRegistration##BaseName() {                                                                                                                                                  \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "-" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, false, false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "-" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, false, false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "-" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  false, false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "-" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  false, false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "+" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "+" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "+" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "+" #Flag3 "-" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  true,  false>), false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "-" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "-" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "-" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "-" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  false, true>),  false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "-" #Flag2 "+" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, false, true,  true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "-" #Flag2 "+" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  false, true,  true>),  false); \
            TCurrentTest::AddTest(#BaseName "-" #Flag1 "+" #Flag2 "+" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<false, true,  true,  true>),  false); \
            TCurrentTest::AddTest(#BaseName "+" #Flag1 "+" #Flag2 "+" #Flag3 "+" #Flag4, static_cast<void (*)(NUnitTest::TTestContext&)>(&BaseName<true,  true,  true,  true>),  false); \
        }                                                                                                                                                                                \
    };                                                                                                                                                                                   \
    static TTestRegistration##BaseName testRegistration##BaseName;                                                                                                                       \
    template<bool Flag1, bool Flag2, bool Flag3, bool Flag4>                                                                                                                             \
    void BaseName(NUnitTest::TTestContext&)


#define Y_UNIT_TEST_DUO Y_UNIT_TEST_COMBINATOR_1
#define Y_UNIT_TEST_QUATRO Y_UNIT_TEST_COMBINATOR_2
#define Y_UNIT_TEST_OCTO Y_UNIT_TEST_COMBINATOR_3
#define Y_UNIT_TEST_SEDECIM Y_UNIT_TEST_COMBINATOR_4

