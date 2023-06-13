#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/driver_lib/version/version.h>
#include "ut_helpers.h"

Y_UNIT_TEST_SUITE(VersionParser) {
    Y_UNIT_TEST(Basic) {
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/branches/kikimr/arcadia"), "branches/kikimr");
    }
}

using EComponentId = NKikimrConfig::TCompatibilityRule;
using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;
using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TYdbVersion;
using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
using TCurrentCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
using TStoredCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TStoredCompatibilityInfo;

Y_UNIT_TEST_SUITE(YdbVersion) {

    void Test(TCurrentCompatibilityInfo current, TCurrentCompatibilityInfo store, bool expected) {
        TString errorReason;
        auto currentPB = current.ToPB();
        auto storePB = store.ToPB();
        auto storedPB = TCompatibilityInfo::MakeStored((ui32)NKikimrConfig::TCompatibilityRule::Test1, &storePB);
        UNIT_ASSERT_EQUAL_C(TCompatibilityInfo::CheckCompatibility(&currentPB, &storedPB, 
            (ui32)EComponentId::Test1, errorReason), expected, errorReason);
    }

    Y_UNIT_TEST(DefaultSameVersion) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultPrevMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 8, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultHotfix) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultCompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 10, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultPrevYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultNewMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultOldMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuild) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuildIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 },
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitOld) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitNew) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 3 },
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1 },
                            .Forbidden = true
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFrom) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromAllOlder) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .UpperLimit = TYdbVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 2 }, 
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(CurrentStoresReadableBy) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(StoredReadableBy) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                true
        );
    }
    Y_UNIT_TEST(StoredReadableByIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(StoredWithRules) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                true
        );
    }
    Y_UNIT_TEST(StoredWithRulesIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 5, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(OldNbsStored) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .Build = "nbs",
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                true
        );
    }
    Y_UNIT_TEST(OldNbsIncompatibleStored) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .Build = "nbs",
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(NewNbsCurrent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .Build = "ydb",
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(NewNbsIncompatibleCurrent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .Build = "ydb",
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(OneAcceptedVersion) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(ForbiddenMinor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(ExtraAndForbidden) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                        },
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(SomeRulesAndOtherForbidden) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                        },
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 4 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 4 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(Component) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Test1,
                        },
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(OtherComponent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Test2,
                        },
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }

    Y_UNIT_TEST(CompatibleWithSelf) {
        auto stored = TCompatibilityInfo::MakeStored(EComponentId::Test1);
        TString errorReason;
        UNIT_ASSERT_C(TCompatibilityInfo::CheckCompatibility(&stored, EComponentId::Test1, errorReason), errorReason);
    }
}

Y_UNIT_TEST_SUITE(OldFormat) {
    void TestOldFormat(TCurrentCompatibilityInfo current, TOldFormat stored, bool expected) {
        TString errorReason;
        auto currentPB = current.ToPB();
        UNIT_ASSERT_EQUAL_C(TCompatibilityInfo::CheckCompatibility(&currentPB, stored, 
            (ui32)EComponentId::Interconnect, errorReason), expected, errorReason);
    }

    Y_UNIT_TEST(SameVersion) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 }
                }, 
                TOldFormat{
                    .Tag = "stable-22-4",
                    .AcceptedTags = { "stable-22-4" }
                },
                true
        );
    }

    Y_UNIT_TEST(DefaultRules) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 22, .Major = 5, .Minor = 1, .Hotfix = 0 }
                }, 
                TOldFormat{
                    .Tag = "stable-22-4",
                    .AcceptedTags = { "stable-22-4" }
                },
                true
        );
    }

    Y_UNIT_TEST(PrevYear) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 22, .Major = 5 },
                            .UpperLimit = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                        },
                    }
                }, 
                TOldFormat{
                    .Tag = "stable-22-5",
                    .AcceptedTags = { "stable-22-5" }
                },
                true
        );
    }

    Y_UNIT_TEST(Trunk) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "trunk"
                }, 
                TOldFormat{
                    .Tag = "trunk",
                    .AcceptedTags = { "trunk" }
                },
                true
        );
    }

    Y_UNIT_TEST(UnexpectedTrunk) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 },
                }, 
                TOldFormat{
                    .Tag = "trunk",
                    .AcceptedTags = { "trunk" }
                },
                false
        );
    }

    Y_UNIT_TEST(TooOld) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 },
                }, 
                TOldFormat{
                    .Tag = "stable-22-2",
                    .AcceptedTags = { "stable-22-2" }
                },
                false
        );
    }

    Y_UNIT_TEST(OldNbs) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 22, .Major = 4 },
                            .UpperLimit = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Interconnect
                        },
                    },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 22, .Major = 4 },
                            .UpperLimit = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Interconnect
                        },
                    }
                }, 
                TOldFormat{
                    .Tag = "stable-22-4",
                    .AcceptedTags = { "stable-22-4" }
                },
                true
        );
    }
}
