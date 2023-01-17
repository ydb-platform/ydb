#include <library/cpp/testing/unittest/registar.h>
#include "version.h"

Y_UNIT_TEST_SUITE(VersionParser) {
    Y_UNIT_TEST(Basic) {
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/branches/kikimr/arcadia"), "branches/kikimr");
    }
}

Y_UNIT_TEST_SUITE(YdbVersion) {
    struct TYdbVersion {
        std::optional<ui32> Year;
        std::optional<ui32> Major;
        std::optional<ui32> Minor;
        std::optional<ui32> Hotfix;

        NKikimrConfig::TYdbVersion ToPB() {
            NKikimrConfig::TYdbVersion res;
            if (Year) {
                res.SetYear(*Year);
            }
            if (Major) {
                res.SetMajor(*Major);
            }
            if (Minor) {
                res.SetMinor(*Minor);
            }
            if (Hotfix) {
                res.SetHotfix(*Hotfix);
            }

            return res;
        }
    };

    struct TCompatibilityRule {
        std::optional<std::string> Build;
        std::optional<TYdbVersion> BottomLimit;
        std::optional<TYdbVersion> UpperLimit;
        std::optional<ui32> ComponentId;
        std::optional<bool> Forbidden;

        NKikimrConfig::TCompatibilityRule ToPB() {
            NKikimrConfig::TCompatibilityRule res;
            if (Build) {
                res.SetBuild(Build->data());
            }
            if (BottomLimit) {
                res.MutableBottomLimit()->CopyFrom(BottomLimit->ToPB());
            }
            if (UpperLimit) {
                res.MutableUpperLimit()->CopyFrom(UpperLimit->ToPB());
            }
            if (ComponentId) {
                res.SetComponentId(*ComponentId);
            }
            if (Forbidden) {
                res.SetForbidden(*Forbidden);
            }

            return res;
        }
    };

    struct TCurrentCompatibilityInformation {
        std::string Build = "ydb";
        std::optional<TYdbVersion> YdbVersion;
        std::vector<TCompatibilityRule> CanLoadFrom;
        std::vector<TCompatibilityRule> StoresReadableBy;

        NKikimrConfig::TCurrentCompatibilityInformation ToPB() {
            NKikimrConfig::TCurrentCompatibilityInformation res;
            res.SetBuild(Build.data());
            if (YdbVersion) {
                res.MutableYdbVersion()->CopyFrom(YdbVersion->ToPB());
            }

            for (auto canLoadFrom : CanLoadFrom) {
                res.AddCanLoadFrom()->CopyFrom(canLoadFrom.ToPB());
            }
            for (auto storesReadableBy : StoresReadableBy) {
                res.AddStoresReadableBy()->CopyFrom(storesReadableBy.ToPB());
            }

            return res;
        }
    };

    struct TStoredCompatibilityInformation {
        std::string Build = "ydb";
        std::optional<TYdbVersion> YdbVersion;
        std::vector<TCompatibilityRule> ReadableBy;

        NKikimrConfig::TStoredCompatibilityInformation ToPB() {
            NKikimrConfig::TStoredCompatibilityInformation res;
            res.SetBuild(Build.data());
            if (YdbVersion) {
                res.MutableYdbVersion()->CopyFrom(YdbVersion->ToPB());
            }

            for (auto readableBy : ReadableBy) {
                res.AddReadableBy()->CopyFrom(readableBy.ToPB());
            }

            return res;
        }
    };

    void Test(TCurrentCompatibilityInformation current, TCurrentCompatibilityInformation store, bool expected) {
        TString errorReason;
        auto currentPB = current.ToPB();
        auto storePB = store.ToPB();
        auto storedPB = MakeStoredCompatibiltyInformation((ui32)NKikimrConfig::TCompatibilityRule::Test1, &storePB);
        UNIT_ASSERT_EQUAL_C(CheckVersionCompatibility(&currentPB, &storedPB, 
            (ui32)NKikimrConfig::TCompatibilityRule::Test1, errorReason), expected, errorReason);
    }

    Y_UNIT_TEST(DefaultSameVersion) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultPrevMajor) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextMajor) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 8, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultHotfix) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultCompatible) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 10, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextYear) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultPrevYear) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultNewMajor) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultOldMajor) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuild) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultDifferentBuildIncompatible) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 },
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitOld) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitNew) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 3 },
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromAllOlder) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .UpperLimit = TYdbVersion{ .Year = 2, .Major = 4, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(CurrentCanLoadFromIncompatible) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 2 }, 
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(CurrentStoresReadableBy) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 },
                    .StoresReadableBy = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(StoredReadableBy) {
        Test(
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 5, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "ydb",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(NewNbsIncompatibleCurrent) {
        Test(
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .Build = "nbs",
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(OneAcceptedVersion) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 }
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(ForbiddenMinor) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3 },
                            .Forbidden = true
                        }
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(ExtraAndForbidden) {
        Test(
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(SomeRulesAndOtherForbidden) {
        Test(
                TCurrentCompatibilityInformation{
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
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(Component) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)NKikimrConfig::TCompatibilityRule::Test1,
                        },
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(OtherComponent) {
        Test(
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 2, .Hotfix = 0 },
                            .ComponentId = (ui32)NKikimrConfig::TCompatibilityRule::Test2,
                        },
                    }
                }, 
                TCurrentCompatibilityInformation{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }

    Y_UNIT_TEST(CompatibleWithSelf) {
        auto* current = GetCurrentCompatibilityInformation();
        auto stored = MakeStoredCompatibiltyInformation((ui32)NKikimrConfig::TCompatibilityRule::Test1);
        TString errorReason;
        UNIT_ASSERT_C(CheckVersionCompatibility(current, &stored, 
                (ui32)NKikimrConfig::TCompatibilityRule::Test1, errorReason), errorReason);
    }
}
