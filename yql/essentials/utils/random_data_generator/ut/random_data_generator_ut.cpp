#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {
struct TPoint {
    double X, Y;
};
} // namespace

namespace NPrivate {
template <>
struct TRandomDataGenerator<TPoint> {
    struct TSettings {
        double Min = -1.0;
        double Max = 1.0;
    };
    static TPoint Generate(IRandomProvider& provider, const TSettings& s) {
        TGeneratorSettings<double> coord{.Min = s.Min, .Max = s.Max};
        return {
            .X = TRandomDataGenerator<double>::Generate(provider, coord),
            .Y = TRandomDataGenerator<double>::Generate(provider, coord),
        };
    }
};
} // namespace NPrivate

namespace {

Y_UNIT_TEST_SUITE(TRandomDataGeneratorTest) {

Y_UNIT_TEST(CountIsRespected) {
    auto rng = CreateDeterministicRandomProvider(42);
    UNIT_ASSERT_EQUAL(GenerateRandomData<i32>(rng, {.Min = 0, .Max = 10}, 0).size(), 0U);
    UNIT_ASSERT_EQUAL(GenerateRandomData<i32>(rng, {.Min = 0, .Max = 10}, 1).size(), 1U);
    UNIT_ASSERT_EQUAL(GenerateRandomData<i32>(rng, {.Min = 0, .Max = 10}, 500).size(), 500U);
}

Y_UNIT_TEST(DefaultSettingsOverload) {
    auto rng = CreateDeterministicRandomProvider(42);
    UNIT_ASSERT_EQUAL(GenerateRandomData<bool>(rng, 100).size(), 100U);
    UNIT_ASSERT_EQUAL(GenerateRandomData<TString>(rng, 100).size(), 100U);
}

Y_UNIT_TEST(BoolAlwaysFalse) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (bool v : GenerateRandomData<bool>(rng, {.TrueProbability = 0.0}, 200)) {
        UNIT_ASSERT(!v);
    }
}

Y_UNIT_TEST(BoolAlwaysTrue) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (bool v : GenerateRandomData<bool>(rng, {.TrueProbability = 1.0}, 200)) {
        UNIT_ASSERT(v);
    }
}

Y_UNIT_TEST(BoolRoughlyHalf) {
    auto rng = CreateDeterministicRandomProvider(42);
    size_t trueCount = 0;
    for (bool v : GenerateRandomData<bool>(rng, {.TrueProbability = 0.5}, 2000)) {
        trueCount += v;
    }
    UNIT_ASSERT_GE(trueCount, 700U);
    UNIT_ASSERT_LE(trueCount, 1300U);
}

Y_UNIT_TEST(IntegerRangeUnsigned) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (ui32 v : GenerateRandomData<ui32>(rng, {.Min = 10, .Max = 20}, 1000)) {
        UNIT_ASSERT_GE(v, 10U);
        UNIT_ASSERT_LT(v, 20U);
    }
}

Y_UNIT_TEST(IntegerRangeSigned) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (i32 v : GenerateRandomData<i32>(rng, {.Min = -50, .Max = 50}, 1000)) {
        UNIT_ASSERT_GE(v, -50);
        UNIT_ASSERT_LT(v, 50);
    }
}

Y_UNIT_TEST(IntegerSingleValue) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (i32 v : GenerateRandomData<i32>(rng, {.Min = 7, .Max = 8}, 100)) {
        UNIT_ASSERT_EQUAL(v, 7);
    }
}

Y_UNIT_TEST(IntegerDefaultSettings) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (ui8 v : GenerateRandomData<ui8>(rng, 500)) {
        UNIT_ASSERT_LT(v, std::numeric_limits<ui8>::max());
    }
}

Y_UNIT_TEST(IntegerFullCoverageOfSmallRange) {
    auto rng = CreateDeterministicRandomProvider(42);
    TSet<i32> seen;
    for (i32 v : GenerateRandomData<i32>(rng, {.Min = 0, .Max = 5}, 10000)) {
        seen.insert(v);
    }
    for (i32 expected = 0; expected < 5; ++expected) {
        UNIT_ASSERT(seen.contains(expected));
    }
    UNIT_ASSERT(!seen.contains(5));
}

Y_UNIT_TEST(FloatRange) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (double v : GenerateRandomData<double>(rng, {.Min = -3.0, .Max = 3.0}, 1000)) {
        UNIT_ASSERT_GE(v, -3.0);
        UNIT_ASSERT_LT(v, 3.0);
    }
}

Y_UNIT_TEST(FloatDefaultRange) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (float v : GenerateRandomData<float>(rng, 500)) {
        UNIT_ASSERT_GE(v, 0.0F);
        UNIT_ASSERT_LT(v, 1.0F);
    }
}

Y_UNIT_TEST(StringLengthBounds) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (const TString& s : GenerateRandomData<TString>(rng, {.MinSize = 3, .MaxSize = 8}, 1000)) {
        UNIT_ASSERT_GE(s.size(), 3U);
        UNIT_ASSERT_LT(s.size(), 8U);
    }
}

Y_UNIT_TEST(StringFixedLength) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (const TString& s : GenerateRandomData<TString>(rng, {.MinSize = 5, .MaxSize = 6}, 100)) {
        UNIT_ASSERT_EQUAL(s.size(), 5U);
    }
}

Y_UNIT_TEST(StringEmptyAllowed) {
    auto rng = CreateDeterministicRandomProvider(42);
    bool hasEmpty = false;
    for (const TString& s : GenerateRandomData<TString>(rng, {.MinSize = 0, .MaxSize = 4}, 2000)) {
        UNIT_ASSERT_LT(s.size(), 4U);
        if (s.empty()) {
            hasEmpty = true;
        }
    }
    UNIT_ASSERT(hasEmpty);
}

Y_UNIT_TEST(StringCustomAlphabet) {
    auto rng = CreateDeterministicRandomProvider(42);
    const TStringBuf alpha = "abc";
    for (const TString& s : GenerateRandomData<TString>(rng, {.MinSize = 1, .MaxSize = 20, .Alphabet = alpha}, 500)) {
        for (char c : s) {
            UNIT_ASSERT(alpha.Contains(c));
        }
    }
}

Y_UNIT_TEST(StringSingleCharAlphabet) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (const TString& s : GenerateRandomData<TString>(rng, {.MinSize = 3, .MaxSize = 6, .Alphabet = "x"}, 100)) {
        for (char c : s) {
            UNIT_ASSERT_EQUAL(c, 'x');
        }
    }
}

Y_UNIT_TEST(MaybeAlwaysNothing) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (const auto& v : GenerateRandomData<TMaybe<i32>>(rng, {.NullProbability = 1.0}, 100)) {
        UNIT_ASSERT(!v.Defined());
    }
}

Y_UNIT_TEST(MaybeAlwaysDefined) {
    auto rng = CreateDeterministicRandomProvider(42);
    for (const auto& v : GenerateRandomData<TMaybe<i32>>(rng, {.NullProbability = 0.0, .Inner = {.Min = 0, .Max = 100}}, 100)) {
        UNIT_ASSERT(v.Defined());
        UNIT_ASSERT_GE(*v, 0);
        UNIT_ASSERT_LT(*v, 100);
    }
}

Y_UNIT_TEST(MaybeRoughlyHalfNothing) {
    auto rng = CreateDeterministicRandomProvider(42);
    size_t nothingCount = 0;
    for (const auto& v : GenerateRandomData<TMaybe<i32>>(rng, {.NullProbability = 0.5, .Inner = {.Min = 0, .Max = 10}}, 2000)) {
        nothingCount += !v.Defined();
    }
    UNIT_ASSERT_GE(nothingCount, 700U);
    UNIT_ASSERT_LE(nothingCount, 1300U);
}

Y_UNIT_TEST(Tuple) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TTuple = std::tuple<i32, TString>;
    auto data = GenerateRandomData<TTuple>(
        rng,
        std::tuple{
            TGeneratorSettings<i32>{.Min = 0, .Max = 100},
            TGeneratorSettings<TString>{.MinSize = 1, .MaxSize = 6}},
        500);
    for (const auto& [num, str] : data) {
        UNIT_ASSERT_GE(num, 0);
        UNIT_ASSERT_LT(num, 100);
        UNIT_ASSERT_GE(str.size(), 1U);
        UNIT_ASSERT_LT(str.size(), 6U);
    }
}

Y_UNIT_TEST(TupleNested) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TInner = std::tuple<bool, ui8>;
    using TOuter = std::tuple<TInner, TString>;
    auto data = GenerateRandomData<TOuter>(
        rng,
        std::tuple{
            std::tuple{TGeneratorSettings<bool>{.TrueProbability = 1.0}, TGeneratorSettings<ui8>{.Min = 0, .Max = 10}},
            TGeneratorSettings<TString>{.MinSize = 1, .MaxSize = 3}},
        100);
    for (const auto& [inner, str] : data) {
        UNIT_ASSERT(std::get<0>(inner));
        UNIT_ASSERT_LT(std::get<1>(inner), 10U);
        UNIT_ASSERT_LT(str.size(), 3U);
    }
}

Y_UNIT_TEST(VariantAlwaysFirst) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TVar = std::variant<i32, TString>;
    TGeneratorSettings<TVar> settings;
    settings.Weights = {1.0, 0.0};
    for (const auto& v : GenerateRandomData<TVar>(rng, settings, 100)) {
        UNIT_ASSERT_EQUAL(v.index(), 0U);
    }
}

Y_UNIT_TEST(VariantAlwaysSecond) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TVar = std::variant<i32, TString>;
    TGeneratorSettings<TVar> settings;
    settings.Weights = {0.0, 1.0};
    for (const auto& v : GenerateRandomData<TVar>(rng, settings, 100)) {
        UNIT_ASSERT_EQUAL(v.index(), 1U);
    }
}

Y_UNIT_TEST(VariantThreeAlternatives) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TVar = std::variant<bool, i32, TString>;
    TGeneratorSettings<TVar> settings;
    settings.Weights = {0.0, 1.0, 0.0};
    for (const auto& v : GenerateRandomData<TVar>(rng, settings, 100)) {
        UNIT_ASSERT_EQUAL(v.index(), 1U);
    }
}

Y_UNIT_TEST(VariantUniformCoverage) {
    auto rng = CreateDeterministicRandomProvider(42);
    using TVar = std::variant<i32, TString>;
    size_t firstCount = 0;
    for (const auto& v : GenerateRandomData<TVar>(rng, 2000)) {
        firstCount += (v.index() == 0);
    }
    UNIT_ASSERT_GE(firstCount, 700U);
    UNIT_ASSERT_LE(firstCount, 1300U);
}

Y_UNIT_TEST(CustomType) {
    auto rng = CreateDeterministicRandomProvider(42);
    auto data = GenerateRandomData<TPoint>(rng, {.Min = 0.0, .Max = 5.0}, 200);
    UNIT_ASSERT_EQUAL(data.size(), 200U);
    for (const auto& p : data) {
        UNIT_ASSERT_GE(p.X, 0.0);
        UNIT_ASSERT_LT(p.X, 5.0);
        UNIT_ASSERT_GE(p.Y, 0.0);
        UNIT_ASSERT_LT(p.Y, 5.0);
    }
}

} // Y_UNIT_TEST_SUITE(TRandomDataGeneratorTest)

} // namespace
} // namespace NKikimr::NMiniKQL
