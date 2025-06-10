#include "yql_langver.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TLangVerTests) {
    Y_UNIT_TEST(IsValidMin) {
        UNIT_ASSERT(IsValidLangVersion(MinLangVersion));
    }

    Y_UNIT_TEST(Parse) {
        TLangVersion v;
        UNIT_ASSERT(!ParseLangVersion("",v));
        UNIT_ASSERT(!ParseLangVersion("2025.01X",v));
        UNIT_ASSERT(!ParseLangVersion("2025-01",v));
        UNIT_ASSERT(!ParseLangVersion("99999.99",v));
        UNIT_ASSERT(ParseLangVersion("2025.01",v));
        UNIT_ASSERT_VALUES_EQUAL(v, MakeLangVersion(2025,1));
        UNIT_ASSERT(ParseLangVersion("9999.99",v));
        UNIT_ASSERT_VALUES_EQUAL(v, MakeLangVersion(9999,99));
    }

    Y_UNIT_TEST(Format) {
        TLangVersionBuffer b;
        TStringBuf s;
        UNIT_ASSERT(!FormatLangVersion(MakeLangVersion(99999, 1), b, s));
        UNIT_ASSERT(!FormatLangVersion(MakeLangVersion(999, 1), b, s));
        UNIT_ASSERT(FormatLangVersion(MakeLangVersion(2025, 1), b, s));
        UNIT_ASSERT_VALUES_EQUAL(s, "2025.01");
        UNIT_ASSERT_VALUES_EQUAL(b[s.Size()], 0);
        UNIT_ASSERT(FormatLangVersion(MakeLangVersion(2025, 12), b, s));
        UNIT_ASSERT_VALUES_EQUAL(s, "2025.12");
        UNIT_ASSERT_VALUES_EQUAL(b[s.Size()], 0);
    }

    Y_UNIT_TEST(Deprecated) {
        UNIT_ASSERT(IsDeprecatedLangVersion(MakeLangVersion(2025,2),MakeLangVersion(2027,1)));
        UNIT_ASSERT(!IsDeprecatedLangVersion(MakeLangVersion(2025,3),MakeLangVersion(2025,1)));
        UNIT_ASSERT(!IsDeprecatedLangVersion(MakeLangVersion(2025,4),MakeLangVersion(2028,1)));
    }

    Y_UNIT_TEST(Unsupported) {
        UNIT_ASSERT(!IsUnsupportedLangVersion(MakeLangVersion(2025,2),MakeLangVersion(2025,1)));
        UNIT_ASSERT(!IsUnsupportedLangVersion(MakeLangVersion(2025,3),MakeLangVersion(2027,1)));
        UNIT_ASSERT(IsUnsupportedLangVersion(MakeLangVersion(2025,4),MakeLangVersion(2028,1)));
        UNIT_ASSERT(IsUnsupportedLangVersion(MakeLangVersion(2025,5),MakeLangVersion(2029,1)));
    }

    Y_UNIT_TEST(Available) {
        UNIT_ASSERT(IsAvailableLangVersion(MakeLangVersion(2025,2),MakeLangVersion(2025,2)));
        UNIT_ASSERT(!IsAvailableLangVersion(MakeLangVersion(2025,3),MakeLangVersion(2025,2)));
    }

    Y_UNIT_TEST(MaxReleasedLangVersionIsValid) {
        UNIT_ASSERT(IsValidLangVersion(GetMaxReleasedLangVersion()));
    }

    Y_UNIT_TEST(MaxLangVersionIsValid) {
        UNIT_ASSERT(IsValidLangVersion(GetMaxLangVersion()));
    }

    Y_UNIT_TEST(MaxVersionIsAboveThanReleased) {
        UNIT_ASSERT(GetMaxLangVersion() > GetMaxReleasedLangVersion());
    }

    Y_UNIT_TEST(BackwardCompatibleFeatureAvailable_All) {
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, MinLangVersion,
            EBackportCompatibleFeaturesMode::All));
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxReleasedLangVersion(),
            EBackportCompatibleFeaturesMode::All));
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxLangVersion(),
            EBackportCompatibleFeaturesMode::All));
    }

    Y_UNIT_TEST(BackwardCompatibleFeatureAvailable_Released) {
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, MinLangVersion,
            EBackportCompatibleFeaturesMode::Released));
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxReleasedLangVersion(),
            EBackportCompatibleFeaturesMode::Released));
        UNIT_ASSERT(!IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxLangVersion(),
            EBackportCompatibleFeaturesMode::Released));
    }

    Y_UNIT_TEST(BackwardCompatibleFeatureAvailable_None) {
        UNIT_ASSERT(IsBackwardCompatibleFeatureAvailable(MinLangVersion, MinLangVersion,
            EBackportCompatibleFeaturesMode::None));
        UNIT_ASSERT(!IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxReleasedLangVersion(),
            EBackportCompatibleFeaturesMode::None));
        UNIT_ASSERT(!IsBackwardCompatibleFeatureAvailable(MinLangVersion, GetMaxLangVersion(),
            EBackportCompatibleFeaturesMode::None));
    }
}

}
