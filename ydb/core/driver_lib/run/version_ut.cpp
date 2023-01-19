#include <library/cpp/testing/unittest/registar.h>
#include "version.h"

class TCompatibilityInfoTest {
public:
    TCompatibilityInfoTest() = delete;

    static void Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent) {
        TCompatibilityInfo::Reset(newCurrent);
    }
};

Y_UNIT_TEST_SUITE(VersionParser) {
    Y_UNIT_TEST(Basic) {
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/branches/kikimr/arcadia"), "branches/kikimr");
    }
}

Y_UNIT_TEST_SUITE(YdbVersion) {
    using EComponentId = NKikimrConfig::TCompatibilityRule;
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

    struct TCurrentCompatibilityInfo {
        std::string Build = "ydb";
        std::optional<TYdbVersion> YdbVersion;
        std::vector<TCompatibilityRule> CanLoadFrom;
        std::vector<TCompatibilityRule> StoresReadableBy;

        NKikimrConfig::TCurrentCompatibilityInfo ToPB() {
            NKikimrConfig::TCurrentCompatibilityInfo res;
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

    struct TStoredCompatibilityInfo {
        std::string Build = "ydb";
        std::optional<TYdbVersion> YdbVersion;
        std::vector<TCompatibilityRule> ReadableBy;

        NKikimrConfig::TStoredCompatibilityInfo ToPB() {
            NKikimrConfig::TStoredCompatibilityInfo res;
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

    void Test(TCurrentCompatibilityInfo current, TCurrentCompatibilityInfo store, bool expected) {
        TString errorReason;
        auto currentPB = current.ToPB();
        auto storePB = store.ToPB();
        auto storedPB = TCompatibilityInfo::MakeStored((ui32)NKikimrConfig::TCompatibilityRule::Test1, &storePB);
        UNIT_ASSERT_EQUAL_C(TCompatibilityInfo::CheckCompatibility(&currentPB, &storedPB, 
            (ui32)NKikimrConfig::TCompatibilityRule::Test1, errorReason), expected, errorReason);
    }

    Y_UNIT_TEST(DefaultSameVersion) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultPrevMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 8, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultHotfix) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultCompatible) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 1, .Hotfix = 10 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 10, .Hotfix = 0 }
                }, 
                true
        );
    }
    Y_UNIT_TEST(DefaultNextYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultPrevYear) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 2, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultNewMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 0 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(DefaultOldMajor) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 2, .Hotfix = 1 }
                }, 
                false
        );
    }
    Y_UNIT_TEST(LimitNew) {
        Test(
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 1, .Hotfix = 3 },
                }, 
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 4, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 5, .Minor = 1, .Hotfix = 0 }
                }, 
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 3, .Minor = 2, .Hotfix = 0 },
                    .CanLoadFrom = {
                        TCompatibilityRule{
                            .BottomLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                            .UpperLimit = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 }
                        }
                    }
                }, 
                TCurrentCompatibilityInfo{
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 2 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(ForbiddenMinor) {
        Test(
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 1, .Minor = 3, .Hotfix = 1 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(ExtraAndForbidden) {
        Test(
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                false
        );
    }
    Y_UNIT_TEST(SomeRulesAndOtherForbidden) {
        Test(
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(Component) {
        Test(
                TCurrentCompatibilityInfo{
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
                    .YdbVersion = TYdbVersion{ .Year = 1, .Major = 2, .Minor = 3, .Hotfix = 0 },
                }, 
                true
        );
    }
    Y_UNIT_TEST(OtherComponent) {
        Test(
                TCurrentCompatibilityInfo{
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
