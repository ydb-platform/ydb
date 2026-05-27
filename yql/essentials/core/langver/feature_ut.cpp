#include "feature.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

TFeature MakeFeature(TLangVersion min, TLangVersion max = UnknownLangVersion) {
    return TFeature{
        .Name = "feat",
        .Description = "Feature",
        .MinLangVer = min,
        .MaxLangVer = max,
    };
}

Y_UNIT_TEST_SUITE(FeatureTests) {

using EBackportCompatibleFeaturesMode::All;
using EBackportCompatibleFeaturesMode::None;
using EBackportCompatibleFeaturesMode::Released;

Y_UNIT_TEST(IsAvailableOn_Unknown_MinMatchesAnyMode) {
    const auto f = MakeFeature(UnknownLangVersion);
    UNIT_ASSERT(IsAvailableOn(MinLangVersion, None, f));
    UNIT_ASSERT(IsAvailableOn(MinLangVersion, Released, f));
    UNIT_ASSERT(IsAvailableOn(MinLangVersion, All, f));
}

Y_UNIT_TEST(IsAvailableOn_Mode_None) {
    const auto f = MakeFeature(MakeLangVersion(2025, 3));
    UNIT_ASSERT(!IsAvailableOn(MakeLangVersion(2025, 2), None, f));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 3), None, f));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 4), None, f));
}

Y_UNIT_TEST(IsAvailableOn_Mode_All) {
    const auto f = MakeFeature(GetMaxLangVersion());
    UNIT_ASSERT(IsAvailableOn(MinLangVersion, All, f));
}

Y_UNIT_TEST(IsAvailableOn_MaxUnknownNeverExpires) {
    const auto f = MakeFeature(MinLangVersion, UnknownLangVersion);
    UNIT_ASSERT(IsAvailableOn(MinLangVersion, None, f));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 4), None, f));
    UNIT_ASSERT(IsAvailableOn(GetMaxLangVersion(), None, f));
}

Y_UNIT_TEST(IsAvailableOn_Max) {
    const auto f = MakeFeature(MinLangVersion, MakeLangVersion(2025, 4));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 3), None, f));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 4), None, f));
    UNIT_ASSERT(!IsAvailableOn(MakeLangVersion(2025, 5), None, f));
}

Y_UNIT_TEST(IsAvailableOn_FailsOnMax) {
    const auto f = MakeFeature(MinLangVersion, MakeLangVersion(2025, 3));
    UNIT_ASSERT(IsAvailableOn(MakeLangVersion(2025, 3), All, f));
    UNIT_ASSERT(!IsAvailableOn(MakeLangVersion(2025, 4), All, f));
}

Y_UNIT_TEST(EnsureIsAvailableOn_Ok) {
    const auto f = MakeFeature(MinLangVersion, MakeLangVersion(2026, 1));
    const auto r = EnsureIsAvailableOn(MakeLangVersion(2025, 3), None, f);
    UNIT_ASSERT(r.has_value());
}

Y_UNIT_TEST(EnsureIsAvailableOn_MinError) {
    const auto f = MakeFeature(MakeLangVersion(2025, 3));
    const auto r = EnsureIsAvailableOn(MakeLangVersion(2025, 2), None, f);
    UNIT_ASSERT(!r.has_value());
    UNIT_ASSERT_STRING_CONTAINS(r.error(), "not available before");
    UNIT_ASSERT_STRING_CONTAINS(r.error(), "2025.03");
}

Y_UNIT_TEST(EnsureIsAvailableOn_MaxError) {
    const auto f = MakeFeature(MinLangVersion, MakeLangVersion(2025, 3));
    const auto r = EnsureIsAvailableOn(MakeLangVersion(2025, 4), None, f);
    UNIT_ASSERT(!r.has_value());
    UNIT_ASSERT_STRING_CONTAINS(r.error(), "not available after");
    UNIT_ASSERT_STRING_CONTAINS(r.error(), "2025.03");
}

Y_UNIT_TEST(EnsureIsAvailableOn_AllModeBypassesMin) {
    const auto f = MakeFeature(GetMaxLangVersion());
    const auto r = EnsureIsAvailableOn(MinLangVersion, All, f);
    UNIT_ASSERT_C(r.has_value(), r.error());
}

} // Y_UNIT_TEST_SUITE(FeatureTests)
