#pragma once

#include <library/cpp/testing/unittest/registar.h>

#define Y_UNIT_TEST_TWIN_IMPL(N, OPT, F)                                                                           \
    template <bool OPT>                                                                                            \
    struct TTestCase##N : public F {                                                                               \
        TTestCase##N() : F() {                                                                                     \
            if constexpr (OPT) { Name_ = #N "+" #OPT; } else { Name_ = #N "-" #OPT; }                              \
        }                                                                                                          \
        static THolder<NUnitTest::TBaseTestCase> CreateOn()  { return ::MakeHolder<TTestCase##N<true>>();  }       \
        static THolder<NUnitTest::TBaseTestCase> CreateOff() { return ::MakeHolder<TTestCase##N<false>>(); }       \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<true>::CreateOn);                                                   \
            TCurrentTest::AddTest(TTestCase##N<false>::CreateOff);                                                 \
        }                                                                                                          \
    };                                                                                                             \
    static const TTestRegistration##N testRegistration##N;                                                         \
    template <bool OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_UNIT_TEST_TWIN(N, OPT) Y_UNIT_TEST_TWIN_IMPL(N, OPT, TCurrentTestCase)
#define Y_UNIT_TEST_TWIN_F(N, OPT, F) Y_UNIT_TEST_TWIN_IMPL(N, OPT, F)

#define Y_UNIT_TEST_QUAD_IMPL(N, OPT1, OPT2, F)                                                                    \
    template <bool OPT1, bool OPT2>                                                                                \
    struct TTestCase##N : public F {                                                                               \
        TTestCase##N() : F() {                                                                                     \
            if constexpr (OPT1) {                                                                                  \
                if constexpr (OPT2) { Name_ = #N "+" #OPT1 "+" #OPT2; } else { Name_ = #N "+" #OPT1 "-" #OPT2; }   \
            } else {                                                                                               \
                if constexpr (OPT2) { Name_ = #N "-" #OPT1 "+" #OPT2; } else { Name_ = #N "-" #OPT1 "-" #OPT2; }   \
            }                                                                                                      \
        }                                                                                                          \
        static THolder<NUnitTest::TBaseTestCase> Create()  { return ::MakeHolder<TTestCase##N<OPT1, OPT2>>();  }   \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<false, false>::Create);                                             \
            TCurrentTest::AddTest(TTestCase##N<true, false>::Create);                                              \
            TCurrentTest::AddTest(TTestCase##N<false, true>::Create);                                              \
            TCurrentTest::AddTest(TTestCase##N<true, true>::Create);                                               \
        }                                                                                                          \
    };                                                                                                             \
    static const TTestRegistration##N testRegistration##N;                                                         \
    template <bool OPT1, bool OPT2>                                                                                \
    void TTestCase##N<OPT1, OPT2>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2) Y_UNIT_TEST_QUAD_IMPL(N, OPT1, OPT2, TCurrentTestCase)
#define Y_UNIT_TEST_QUAD_F(N, OPT1, OPT2, F) Y_UNIT_TEST_QUAD_IMPL(N, OPT1, OPT2, F)
