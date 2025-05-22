#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/driver_lib/version/version.h>
#include "ut_helpers.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(VersionParser) {
    Y_UNIT_TEST(Basic) {
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/branches/kikimr/arcadia"), "branches/kikimr");
    }
}

using TComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;
using EComponentId = NKikimrConfig::TCompatibilityRule;
using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;
using TVersion = TCompatibilityInfo::TProtoConstructor::TVersion;
using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
using TCurrentCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
using TStoredCompatibilityInfo = TCompatibilityInfo::TProtoConstructor::TStoredCompatibilityInfo;

constexpr bool PRINT_HUMAN_READABLE = false;
constexpr bool PRINT_JSON = false;

// #define HUMAN_READABLE_PRINT_TESTS

Y_UNIT_TEST_SUITE(YdbVersion) {

    void Test(TCurrentCompatibilityInfo current, TCurrentCompatibilityInfo store, bool expected,
            TComponentId componentId = EComponentId::Test1) {
        TString errorReason;
        auto currentPB = current.ToPB();
        auto storePB = store.ToPB();
        auto storedPB = CompatibilityInfo.MakeStored(componentId, &storePB);
        UNIT_ASSERT_EQUAL_C(CompatibilityInfo.CheckCompatibility(&currentPB, &storedPB, 
            componentId, errorReason), expected, errorReason);

        if (PRINT_HUMAN_READABLE) {
            Cerr << CompatibilityInfo.PrintHumanReadable(&currentPB) << Endl << Endl;
        }
        if (PRINT_JSON) {
            Cerr << CompatibilityInfo.PrintJson(&currentPB) << Endl << Endl;
        }
    }

    Y_UNIT_TEST(DefaultSameVersion) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultPrevMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 8, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultHotfix) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultCompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 10, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultPrevYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultNewMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultOldMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuild) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuildIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 },
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitOld) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitNew) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 3 },
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 2, .Minor = 1 },
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromAllOlder) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .UpperLimit = TVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 2 }, 
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(CurrentStoresReadableBy) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(StoredReadableBy) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                true
        );
    }
    Y_UNIT_TEST(StoredReadableByIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(StoredWithRules) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 4, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                true
        );
    }
    Y_UNIT_TEST(StoredWithRulesIncompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 5, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 }
                        }
                    }
                }, 
                false
        );
    }
    Y_UNIT_TEST(OldNbsStored) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                            .ComponentId = EComponentId::Interconnect,
                        }
                    }
                }, 
                true,
                EComponentId::Interconnect
        );
    }
    Y_UNIT_TEST(OldNbsIncompatibleStored) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 3, .Hotfix = 1 },
                            .ComponentId = EComponentId::Interconnect,
                        }
                    }
                }, 
                false,
                EComponentId::Interconnect
        );
    }
    Y_UNIT_TEST(NewNbsCurrent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .Application = "ydb",
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = EComponentId::Interconnect,
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true,
                EComponentId::Interconnect
        );
    }
    Y_UNIT_TEST(NewNbsIncompatibleCurrent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .Application = "ydb",
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = EComponentId::Interconnect,
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(OneAcceptedVersion) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(ForbiddenMinor) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultRulesWithExtraForbidden) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 1, .Minor = 4 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(ExtraAndForbidden) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                        },
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 2, .Minor = 3 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 2, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(SomeRulesAndOtherForbidden) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                        },
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 2, .Minor = 4 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 2, .Minor = 4 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(Component) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Test1,
                        },
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(OtherComponent) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)EComponentId::Test2,
                        },
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }

    Y_UNIT_TEST(YDBAndNbs) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                    .Version = TVersion{ .Year = 23, .Major = 3, .Minor = 2, .Hotfix = 0 },
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 24, .Major = 2, .Minor = 3, .Hotfix = 0 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                            .UpperLimit = TVersion{ .Year = 24, .Major = 2 },
                            .ComponentId = EComponentId::Interconnect,
                        },
                    }
                }, 
                true,
                EComponentId::Interconnect
        );
    }

    Y_UNIT_TEST(DifferentYdbVersionsWithNBSRules) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 24, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                            .UpperLimit = TVersion{ .Year = 24, .Major = 3 },
                        },
                    },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                            .UpperLimit = TVersion{ .Year = 24, .Major = 3 },
                        },
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 24, .Major = 2, .Minor = 3, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                            .UpperLimit = TVersion{ .Year = 24, .Major = 2 },
                        },
                    },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .Application = "nbs",
                            .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                            .UpperLimit = TVersion{ .Year = 24, .Major = 2 },
                        },
                    }
                }, 
                true
        );
    }

    Y_UNIT_TEST(TrunkYDBAndNbs) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "nbs",
                }, 
                true,
                EComponentId::Interconnect
        );
    }
    Y_UNIT_TEST(TrunkAndStable) {
        Test(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                }, 
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 24, .Major = 3, .Minor = 1, .Hotfix = 0 },
                }, 
                false
        );
    }

    Y_UNIT_TEST(CompatibleWithSelf) {
        auto stored = CompatibilityInfo.MakeStored(EComponentId::Test1);
        TString errorReason;
        UNIT_ASSERT_C(CompatibilityInfo.CheckCompatibility(&stored, EComponentId::Test1, errorReason), errorReason);
    }

    enum class EPrintAs {
        Proto = 0,
        HumanReadable,
        Json,
    };

    void PrintCompatibilityInfo(const NKikimrConfig::TCurrentCompatibilityInfo& current, EPrintAs printAs = EPrintAs::Proto) {
        switch (printAs) {
        case EPrintAs::Proto: {
            TString str;
            google::protobuf::TextFormat::PrintToString(current, &str);
            Cerr << str << Endl;
            break;
        }
        case EPrintAs::HumanReadable: {
            Cerr << CompatibilityInfo.PrintHumanReadable(&current) << Endl;
            break;
        }
        case EPrintAs::Json: {
            Cerr << CompatibilityInfo.PrintJson(&current) << Endl;
            break;
        }
        }
    }

    Y_UNIT_TEST(PrintCurrentVersionProto) {
        PrintCompatibilityInfo(*CompatibilityInfo.GetCurrent());
    }

#ifdef HUMAN_READABLE_PRINT_TESTS
    Y_UNIT_TEST(PrintTrunk) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintStable) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 23, .Major = 3, .Minor = 8, .Hotfix = 4 },
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintYdbAndNbs) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 24, .Major = 2, .Minor = 2, .Hotfix = 0 },
                .CanConnectTo = {
                    TCompatibilityRule{
                        .Application = "nbs",
                        .LowerLimit = TVersion{ .Year = 23, .Major = 3 },
                        .UpperLimit = TVersion{ .Year = 24, .Major = 2 },
                        .ComponentId = EComponentId::Interconnect,
                    },
                },
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintBadPDisk) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 23, .Major = 4, .Minor = 5, .Hotfix = 1 },
                .StoresReadableBy = {
                    TCompatibilityRule{
                        .Application = "ydb",
                        .LowerLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5, .Hotfix = 0 },
                        .UpperLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5, .Hotfix = 0 },
                        .ComponentId = EComponentId::PDisk,
                        .Forbidden = true,
                    },
                }
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintBadWholeMinorPDisk) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 23, .Major = 4, .Minor = 5, .Hotfix = 1 },
                .StoresReadableBy = {
                    TCompatibilityRule{
                        .Application = "ydb",
                        .LowerLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5 },
                        .UpperLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5 },
                        .ComponentId = EComponentId::PDisk,
                        .Forbidden = true,
                    },
                }
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintCanConnectToWMinor) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 24, .Major = 4, .Minor = 2, .Hotfix = 0 },
                .CanConnectTo = {
                    TCompatibilityRule{
                        .Application = "nbs",
                        .LowerLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5 },
                        .UpperLimit = TVersion{ .Year = 24, .Major = 4 },
                        .ComponentId = EComponentId::Interconnect,
                    },
                }
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }

    Y_UNIT_TEST(PrintCanConnectToWHotfix) {
        PrintCompatibilityInfo(
            TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TVersion{ .Year = 24, .Major = 4, .Minor = 2, .Hotfix = 0 },
                .CanConnectTo = {
                    TCompatibilityRule{
                        .Application = "nbs",
                        .LowerLimit = TVersion{ .Year = 23, .Major = 4, .Minor = 5, .Hotfix = 1 },
                        .UpperLimit = TVersion{ .Year = 24, .Major = 4 },
                        .ComponentId = EComponentId::Interconnect,
                    },
                }
            }.ToPB(),
            EPrintAs::HumanReadable
        );
    }
#endif
}

Y_UNIT_TEST_SUITE(OldFormat) {
    void TestOldFormat(TCurrentCompatibilityInfo current, TOldFormat stored, bool expected) {
        TString errorReason;
        auto currentPB = current.ToPB();
        UNIT_ASSERT_EQUAL_C(CompatibilityInfo.CheckCompatibility(&currentPB, stored, 
            EComponentId::Interconnect, errorReason), expected, errorReason);
    }

    Y_UNIT_TEST(SameVersion) {
        TestOldFormat(
                TCurrentCompatibilityInfo{
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 }
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 22, .Major = 5, .Minor = 1, .Hotfix = 0 }
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 22, .Major = 5 },
                            .UpperLimit = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                        },
                    },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 22, .Major = 5 },
                            .UpperLimit = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                        },
                    },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 22, .Major = 5 },
                            .UpperLimit = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
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
                    .Application = "ydb"
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 },
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 22, .Major = 4, .Minor = 1, .Hotfix = 0 },
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
                    .Application = "ydb",
                    .Version = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
                    .CanConnectTo = {
                        TCompatibilityRule{
                            .LowerLimit = TVersion{ .Year = 22, .Major = 4 },
                            .UpperLimit = TVersion{ .Year = 23, .Major = 1, .Minor = 1, .Hotfix = 0 },
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
