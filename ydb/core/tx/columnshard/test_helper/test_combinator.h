#pragma once

#include <functional>
#include <tuple>
#include <utility>

// Helpers for parametrized tests over Cartesian product of enum values
// Requires GetEnumAllValues<T>() for each enum type (e.g. via GENERATE_ENUM_SERIALIZATION)

template <class... Es>
static inline TString BuildParamTestName(const char* base, const Es&... es) {
    TString s = base;
    ((s += "-", s += ToString(es)), ...);
    return s;
}

template <class F>
static inline void ForEachProductRanges(F&& f) {
    std::invoke(std::forward<F>(f));
}

template <class F, class FirstRange, class... RestRanges>
static inline void ForEachProductRanges(F&& f, const FirstRange& first, const RestRanges&... rest) {
    for (auto&& x : first) {
        auto bound = std::bind_front(std::forward<F>(f), x);
        ForEachProductRanges(std::move(bound), rest...);
    }
}

template <class... Enums, class F>
static inline void ForEachEnums(F&& f) {
    ForEachProductRanges(std::forward<F>(f), GetEnumAllValues<Enums>()...);
}

template <class Tuple, size_t... I>
static inline TString BuildParamTestNameFromTupleImpl(const char* base, const Tuple& t, std::index_sequence<I...>) {
    return BuildParamTestName(base, std::get<I>(t)...);
}

template <class Tuple>
static inline TString BuildParamTestNameFromTuple(const char* base, const Tuple& t) {
    return BuildParamTestNameFromTupleImpl(base, t, std::make_index_sequence<std::tuple_size<Tuple>::value>{});
}

#define Y_UNIT_TEST_ALL_ENUM_VALUES_VAR(N, ...) \
    struct TTestCase##N: public TCurrentTestCase { \
        using Types = std::tuple<__VA_ARGS__>; \
        Types Args; \
        TString ParametrizedTestName; \
        explicit TTestCase##N(Types args) \
            : Args(std::move(args)) \
            , ParametrizedTestName(BuildParamTestNameFromTuple(#N, Args)) { \
            Name_ = ParametrizedTestName.c_str(); \
        } \
        static THolder<NUnitTest::TBaseTestCase> Create(Types args) { \
            return ::MakeHolder<TTestCase##N>(std::move(args)); \
        } \
        void Execute_(NUnitTest::TTestContext&) override; \
        template <size_t I> \
        decltype(auto) Arg() const { \
            return std::get<I>(Args); \
        } \
    }; \
    struct TTestRegistration##N { \
        TTestRegistration##N() { \
            ForEachEnums<__VA_ARGS__>([&](auto... items) { \
                TCurrentTest::AddTest([=] { \
                    return TTestCase##N::Create(typename TTestCase##N::Types(items...)); \
                }); \
            }); \
        } \
    }; \
    static TTestRegistration##N testRegistration##N; \
    void TTestCase##N::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

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

