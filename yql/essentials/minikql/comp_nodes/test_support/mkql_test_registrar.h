#pragma once

#include <library/cpp/testing/unittest/registar.h>

#if defined(_msan_enabled_) || defined(_ubsan_enabled_) || defined(WITH_VALGRIND)
    #define Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT)                                                                     \
        template <bool OPT>                                                                                            \
        void N(NUnitTest::TTestContext&);                                                                              \
        struct TTestRegistration##N {                                                                                  \
            TTestRegistration##N() {                                                                                   \
                TCurrentTest::AddTest(#N "-" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false>), false); \
            }                                                                                                          \
        };                                                                                                             \
        static TTestRegistration##N testRegistration##N; /* NOLINT(misc-use-anonymous-namespace) */
#else
    #define Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT)                                                                     \
        template <bool OPT>                                                                                            \
        void N(NUnitTest::TTestContext&);                                                                              \
        struct TTestRegistration##N {                                                                                  \
            TTestRegistration##N() {                                                                                   \
                TCurrentTest::AddTest(#N "-" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false>), false); \
                TCurrentTest::AddTest(#N "+" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true>), false);  \
            }                                                                                                          \
        };                                                                                                             \
        static TTestRegistration##N testRegistration##N; /* NOLINT(misc-use-anonymous-namespace) */
#endif

#define Y_UNIT_TEST_TWIN(N, OPT)           \
    Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT) \
    template <bool OPT>                    \
    void N(NUnitTest::TTestContext&)

#define Y_UNIT_TEST_LLVM(N) Y_UNIT_TEST_TWIN(N, LLVM)
#define Y_UNIT_TEST_LLVM_SPILLING(N) Y_UNIT_TEST_QUAD(N, LLVM, SPILLING)

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2)                                                                                              \
    template <bool OPT1, bool OPT2>                                                                                                  \
    void N(NUnitTest::TTestContext&);                                                                                                \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false);   \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N; /* NOLINT(misc-use-anonymous-namespace) */                                      \
    template <bool OPT1, bool OPT2>                                                                                                  \
    void N(NUnitTest::TTestContext&)
