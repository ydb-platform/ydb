#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_dbase_apply.h>

namespace NKikimr {
namespace NTable {

namespace {

    struct TModel {
        using ECodec = NKikimr::NTable::NPage::ECodec;
        using ECache = NKikimr::NTable::NPage::ECache;
        using ECacheMode = NKikimr::NTable::NPage::ECacheMode;

        enum ETokens : ui32 {
            TableId = 1,

            ColId1 = 1,
            ColId2 = 2,

            GroupId1 = NKikimr::NTable::TColumn::LeaderFamily,
            GroupId2 = GroupId1 + 3,

            StoreIdDef = TScheme::DefaultRoom,
            StoreIdOut = StoreIdDef + 5,

            ChannelDef  = TScheme::DefaultChannel,
            ChannelOut  = ChannelDef + 7,
            ChannelBlobs  = ChannelDef + 11,
        };

        TModel() { };

        TAlter Build() const
        {
            TAlter bld;

            using namespace NKikimr;

            bld.AddTable("table1", TableId);
            bld.SetRoom(TableId, StoreIdOut, ChannelOut, {ChannelBlobs}, ChannelOut);
            bld.AddFamily(TableId, GroupId1, StoreIdDef);
            bld.SetFamilyCompression(TableId, GroupId1, ECodec::LZ4);
            bld.SetFamilyCacheMode(TableId, GroupId1, ECacheMode::TryKeepInMemory);
            bld.SetFamilyCache(TableId, GroupId1, ECache::Ever);
            bld.AddFamily(TableId, GroupId2, StoreIdOut);
            bld.AddColumn(TableId, "key", ColId1, NScheme::TSmallBoundedString::TypeId, false, false);
            bld.AddColumn(TableId, "value", ColId2, NScheme::TUint32::TypeId, false, false);
            bld.AddColumnToKey(TableId, ColId1);
            bld.AddColumnToFamily(TableId, ColId1, GroupId1);
            bld.AddColumnToFamily(TableId, ColId2, GroupId2);

            return bld;
        }

        void Check(const TScheme &scheme) const
        {
            UNIT_ASSERT(scheme.Tables.size() == 1);

            auto it= scheme.Tables.find(TableId);

            UNIT_ASSERT(it != scheme.Tables.end());

            const auto &table = it->second;

            UNIT_ASSERT(table.Columns.size() ==  2);
            UNIT_ASSERT(table.Families.size() ==  2);
            UNIT_ASSERT(table.Rooms.size() ==  2);

            if (const auto *family = table.Families.FindPtr(GroupId1)) {
                UNIT_ASSERT(family->Room == StoreIdDef);
                UNIT_ASSERT(family->Cache == ECache::Ever);
                UNIT_ASSERT(family->Codec == ECodec::LZ4);
                UNIT_ASSERT(family->Large == Max<ui32>());
                UNIT_ASSERT(family->CacheMode == ECacheMode::TryKeepInMemory);
            } else {
                UNIT_FAIL("Cannot find primary group in scheme");
            }

            UNIT_ASSERT(table.Families.find(GroupId2)->second.Room == StoreIdOut);
            UNIT_ASSERT(table.Rooms.find(StoreIdDef)->second.Main == ChannelDef);
            UNIT_ASSERT(table.Rooms.find(StoreIdDef)->second.Blobs == std::vector<ui8>{ChannelDef});
            UNIT_ASSERT(table.Rooms.find(StoreIdOut)->second.Main == ChannelOut);
            UNIT_ASSERT(table.Rooms.find(StoreIdOut)->second.Blobs == std::vector<ui8>{ChannelBlobs});
        }
    };

    struct TCompare {
        using TTableInfo = TScheme::TTableInfo;

        static bool IsTheSame(const TScheme::TRoom &a, const TScheme::TRoom &b) noexcept
        {
            return
                a.Main == b.Main
                && a.Blobs == b.Blobs
                && a.Outer == b.Outer;
        }

        static bool IsTheSame(const TScheme::TFamily &a, const TScheme::TFamily &b) noexcept
        {
            return
                a.Room == b.Room
                && a.Cache == b.Cache
                && a.Codec == b.Codec
                && a.Small == b.Small
                && a.Large == b.Large
                && a.CacheMode == b.CacheMode;
        }

        static bool IsTheSame(const TColumn &a, const TColumn &b) noexcept
        {
            return
                a.Id == b.Id
                && a.PType == b.PType
                && a.PTypeMod == b.PTypeMod
                && a.KeyOrder == b.KeyOrder
                && a.Name == b.Name
                && a.Family == b.Family
                && a.NotNull == b.NotNull;
        }

        bool Do(ui32 table, const TScheme &left, const TScheme &right) const
        {
            auto le = left.Tables.find(table);
            auto ri = right.Tables.find(table);

            return
                le != left.Tables.end()
                && ri != right.Tables.end()
                && Do(le->second, ri->second);
        }

        bool Do(const TTableInfo &base, const TTableInfo &with) const
        {
            return
                base.Id == with.Id
                && base.Name == with.Name
                && ByValues(base.Columns, with.Columns)
                && ByValues(base.Families, with.Families)
                && ByValues(base.Rooms, with.Rooms)
                && base.TieringPolicy == with.TieringPolicy;
        }

        template<typename TValues>
        static bool ByValues(const TValues &base, const TValues &with)
        {
            ui32 errors = (base.size() == with.size() ? 0 : 1);

            for (auto &it: base) {
                const auto &on = with.find(it.first);

                if (on == with.end()) {
                    // Sometimes will be replaced with log line
                } else if (!IsTheSame(it.second, on->second)) {
                    // Sometimes will be replaced with log line
                } else
                    continue;

                errors++;
            }

            return errors == 0;
        }
    };
}

Y_UNIT_TEST_SUITE(TScheme) {
    Y_UNIT_TEST(Shapshot)
    {
        const TModel model;

        TScheme origin;

        TSchemeModifier(origin).Apply(*model.Build().Flush());

        model.Check(origin);

        TScheme restored;

        TSchemeModifier(restored).Apply(*origin.GetSnapshot());

        model.Check(restored);

        UNIT_ASSERT(!TCompare().Do(TModel::TableId, origin, {}));
        UNIT_ASSERT(TCompare().Do(TModel::TableId, origin, origin));
        UNIT_ASSERT(TCompare().Do(TModel::TableId, origin, restored));
    }

    Y_UNIT_TEST(Delta)
    {
        const TModel model;

        TAlter delta = model.Build();

        TScheme restored;

        TSchemeModifier(restored).Apply(*delta.Flush());

        model.Check(restored);
    }

    Y_UNIT_TEST(Policy)
    {
        auto delta = TModel().Build();

        TIntrusivePtr<TCompactionPolicy> policy = new TCompactionPolicy();
        policy->InMemSizeToSnapshot = 1234;
        policy->InMemStepsToSnapshot = 100;
        policy->InMemForceStepsToSnapshot = 200;
        policy->InMemForceSizeToSnapshot = 5678;
        policy->InMemResourceBrokerTask = NLocalDb::LegacyQueueIdToTaskName(0);
        policy->ReadAheadHiThreshold = 100000;
        policy->ReadAheadLoThreshold = 50000;
        policy->Generations.push_back({150, 3, 4, 250, NLocalDb::LegacyQueueIdToTaskName(1), true});
        policy->Generations.push_back({550, 7, 8, 950, NLocalDb::LegacyQueueIdToTaskName(2), false});

        delta.SetCompactionPolicy(TModel::TableId, *policy);

        TAutoPtr<TScheme> scheme = new TScheme();
        TSchemeModifier(*scheme).Apply(*delta.Flush());

        auto snapshot = scheme->GetSnapshot();

        TAutoPtr<TScheme> scheme2 = new TScheme();
        TSchemeModifier(*scheme2).Apply(*snapshot);

        if (auto &policy = scheme2->GetTableInfo(TModel::TableId)->CompactionPolicy) {
            UNIT_ASSERT_VALUES_EQUAL(policy->InMemSizeToSnapshot, 1234);
            UNIT_ASSERT_VALUES_EQUAL(policy->InMemStepsToSnapshot, 100);
            UNIT_ASSERT_VALUES_EQUAL(policy->InMemForceStepsToSnapshot, 200);
            UNIT_ASSERT_VALUES_EQUAL(policy->InMemForceSizeToSnapshot, 5678);
            UNIT_ASSERT_VALUES_EQUAL(policy->ReadAheadHiThreshold, 100000);
            UNIT_ASSERT_VALUES_EQUAL(policy->ReadAheadLoThreshold, 50000);
            UNIT_ASSERT_VALUES_EQUAL(policy->Generations.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(policy->Generations[0].SizeToCompact, 150);
            UNIT_ASSERT_VALUES_EQUAL(policy->Generations[0].KeepInCache, true);
            UNIT_ASSERT_VALUES_EQUAL(policy->Generations[1].ForceSizeToCompact, 950);
            UNIT_ASSERT_VALUES_EQUAL(policy->Generations[1].KeepInCache, false);
        } else {
            UNIT_ASSERT(false);
        }
    }

    Y_UNIT_TEST(MalformedFamilySettings)
    {
        using namespace NTable::NPage;

        // Values out of the enum range must not be truncated to valid ones
        auto apply = [](auto&& malform) {
            TScheme scheme;
            TSchemeModifier(scheme).Apply(*TModel().Build().Flush());

            TAlter alter;
            alter.SetFamily(TModel::TableId, TModel::GroupId1, ECache::None, ECodec::Plain);
            alter.SetFamilyCacheMode(TModel::TableId, TModel::GroupId1, ECacheMode::Regular);
            auto changes = alter.Flush();
            for (size_t i = 0; i < changes->DeltaSize(); ++i) {
                malform(*changes->MutableDelta(i));
            }
            TSchemeModifier(scheme).Apply(*changes);
        };

        apply([](auto&) { });
        UNIT_ASSERT_EXCEPTION_CONTAINS(apply([](auto& delta) { delta.SetCodec(256); }),
            yexception, "Invalid page encoding code value 256");
        UNIT_ASSERT_EXCEPTION_CONTAINS(apply([](auto& delta) { delta.SetCache(256); }),
            yexception, "Invalid pages cache policy value 256");
        UNIT_ASSERT_EXCEPTION_CONTAINS(apply([](auto& delta) { delta.SetCacheMode(65536); }),
            yexception, "Invalid cache mode value 65536");
    }
}

namespace {

    using ECodec = NPage::ECodec;
    using ECache = NPage::ECache;
    using ECacheMode = NPage::ECacheMode;

    constexpr ui32 TableId = TModel::TableId;
    constexpr ui32 ColdRoom = TModel::StoreIdOut + 1;
    // Tiers are families without columns, TModel families with columns are 0 and 3
    constexpr ui32 HddTier = 1;
    constexpr ui32 ArchiveTier = 2;
    constexpr ui32 ExtraTier = 4;
    constexpr ui32 TimeColumn = 3;

    void AddTier(TAlter& alter, ui32 tier) {
        alter.AddFamily(TableId, tier, ColdRoom);
        alter.SetFamilyCompression(TableId, tier, ECodec::LZ4);
    }

    // TModel table with a timestamp column, a cold room and two tiers
    TAlter BuildTiered() {
        TAlter alter = TModel().Build();
        alter.AddColumn(TableId, "created_at", TimeColumn, NScheme::NTypeIds::Timestamp, false, false);
        alter.AddColumnToFamily(TableId, TimeColumn, TModel::GroupId1);
        alter.SetRoom(TableId, ColdRoom, TModel::ChannelOut + 1, {TModel::ChannelBlobs + 1}, TModel::ChannelOut + 1);
        AddTier(alter, HddTier);
        AddTier(alter, ArchiveTier);
        return alter;
    }

    TTierRange Interval(TDuration offset) {
        return TTierRange{ .To = TTierRelativeBound{ offset }, .ToInclusive = true };
    }

    TTieringPolicy RelativePolicy() {
        TTieringPolicy policy;
        policy.ColumnTag = TimeColumn;
        policy.Rules.push_back(TTierRule{ .TierId = ArchiveTier, .Ranges = { Interval(TDuration::Days(3)) } });
        policy.Rules.push_back(TTierRule{ .TierId = HddTier, .Ranges = { Interval(TDuration::Days(1)) } });
        return policy;
    }

    TTieringPolicy AbsolutePolicy() {
        // 2024 and the first half of 2025 to HDD, everything before 1970 to archive
        const i64 y2024 = 1704067200ll * 1000000;
        const i64 y2025 = 1735689600ll * 1000000;
        const i64 mid2025 = 1751328000ll * 1000000;
        TTieringPolicy policy;
        policy.ColumnTag = TimeColumn;
        policy.Rules.push_back(TTierRule{ .TierId = HddTier, .Ranges = {
            TTierRange{ .From = TTierAbsoluteBound{ y2024 }, .To = TTierAbsoluteBound{ y2025 } },
            TTierRange{ .From = TTierAbsoluteBound{ y2025 }, .To = TTierAbsoluteBound{ mid2025 }, .ToInclusive = true },
        } });
        policy.Rules.push_back(TTierRule{ .TierId = ArchiveTier, .Ranges = {
            TTierRange{ .To = TTierAbsoluteBound{ -1 } },
        } });
        return policy;
    }

    TScheme Apply(TAlter alter) {
        TScheme scheme;
        TSchemeModifier(scheme).Apply(*alter.Flush());
        return scheme;
    }

    const TScheme::TTableInfo& Table(const TScheme& scheme) {
        const auto* table = scheme.GetTableInfo(TableId);
        UNIT_ASSERT(table);
        return *table;
    }

    // TAlter writes every change as a separate record, merge them into one
    TAutoPtr<TSchemeChanges> AsOneRecord(TAlter& alter) {
        auto changes = alter.Flush();
        UNIT_ASSERT(changes->DeltaSize() > 1);
        for (size_t i = 1; i < changes->DeltaSize(); ++i) {
            changes->MutableDelta(0)->MergeFrom(changes->GetDelta(i));
        }
        while (changes->DeltaSize() > 1) {
            changes->MutableDelta()->RemoveLast();
        }
        return changes;
    }

} // namespace

Y_UNIT_TEST_SUITE(TSchemeTiering) {

    Y_UNIT_TEST(PolicyProtoRoundTrip) {
        for (auto policy : {RelativePolicy(), AbsolutePolicy()}) {
            policy.Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
            NTabletFlatScheme::TTieringPolicySettings proto;
            TieringPolicyToProto(policy, proto);
            UNIT_ASSERT(TieringPolicyFromProto(proto) == policy);
        }
    }

    Y_UNIT_TEST(PolicyProtoReplace) {
        // Serialization replaces the previous content of the proto
        NTabletFlatScheme::TTieringPolicySettings proto;
        TieringPolicyToProto(RelativePolicy(), proto);
        TieringPolicyToProto(RelativePolicy(), proto);
        UNIT_ASSERT(TieringPolicyFromProto(proto) == RelativePolicy());

        TieringPolicyToProto(AbsolutePolicy(), proto);
        UNIT_ASSERT(TieringPolicyFromProto(proto) == AbsolutePolicy());
    }

    Y_UNIT_TEST(TiersAndPolicy) {
        auto alter = BuildTiered();
        alter.SetTieringPolicy(TableId, RelativePolicy());
        const TScheme scheme = Apply(alter);
        const auto& table = Table(scheme);

        // Tiers are families of the table, two with columns and two tiers
        UNIT_ASSERT_VALUES_EQUAL(table.Families.size(), 4u);
        const auto& hdd = table.Families.at(HddTier);
        UNIT_ASSERT_VALUES_EQUAL(hdd.Room, ColdRoom);
        UNIT_ASSERT(hdd.Codec == ECodec::LZ4);
        UNIT_ASSERT(hdd.Cache == ECache::None);
        UNIT_ASSERT(hdd.CacheMode == ECacheMode::Regular);
        UNIT_ASSERT(table.TieringPolicy == RelativePolicy());
    }

    Y_UNIT_TEST(Snapshot) {
        for (const auto& policy : {RelativePolicy(), AbsolutePolicy()}) {
            auto alter = BuildTiered();
            AddTier(alter, ExtraTier);
            alter.DropFamily(TableId, ExtraTier);
            alter.SetTieringPolicy(TableId, policy);
            const TScheme origin = Apply(alter);

            TScheme restored;
            TSchemeModifier(restored).Apply(*origin.GetSnapshot());

            UNIT_ASSERT(TCompare().Do(TableId, origin, restored));
            UNIT_ASSERT(Table(restored).TieringPolicy == policy);
            UNIT_ASSERT(Table(restored).Families.contains(HddTier));
            UNIT_ASSERT(Table(restored).Families.contains(ArchiveTier));
            UNIT_ASSERT(!Table(restored).Families.contains(ExtraTier));
        }
    }

    Y_UNIT_TEST(AlterTiersAndPolicy) {
        auto alter = BuildTiered();
        TScheme scheme = Apply(alter);

        // Change a tier, set and reset the policy, drop an unused tier
        TAlter change;
        change.SetFamilyCache(TableId, HddTier, ECache::Once);
        change.SetTieringPolicy(TableId, RelativePolicy());
        UNIT_ASSERT(TSchemeModifier(scheme).Apply(*change.Flush()));
        UNIT_ASSERT(Table(scheme).Families.at(HddTier).Cache == ECache::Once);
        UNIT_ASSERT(Table(scheme).TieringPolicy);

        // Applying the same changes again is a no-op
        TAlter same;
        same.SetFamilyCache(TableId, HddTier, ECache::Once);
        same.SetTieringPolicy(TableId, RelativePolicy());
        UNIT_ASSERT(!TSchemeModifier(scheme).Apply(*same.Flush()));

        TAlter reset;
        reset.ResetTieringPolicy(TableId);
        reset.DropFamily(TableId, ArchiveTier);
        UNIT_ASSERT(TSchemeModifier(scheme).Apply(*reset.Flush()));
        UNIT_ASSERT(!Table(scheme).TieringPolicy);
        UNIT_ASSERT(!Table(scheme).Families.contains(ArchiveTier));
        UNIT_ASSERT(Table(scheme).Families.contains(HddTier));

        // Dropping a missing family is a no-op
        TAlter missing;
        missing.DropFamily(TableId, ArchiveTier);
        UNIT_ASSERT(!TSchemeModifier(scheme).Apply(*missing.Flush()));
    }

    Y_UNIT_TEST(Rollback) {
        auto alter = BuildTiered();
        TScheme scheme = Apply(alter);
        const TScheme::TTableInfo before = Table(scheme);

        TAlter change;
        change.SetTieringPolicy(TableId, RelativePolicy());
        change.ResetTieringPolicy(TableId);
        change.DropFamily(TableId, ArchiveTier);

        TSchemeRollbackState rollback;
        TSchemeModifier(scheme, &rollback).Apply(*change.Flush());

        // The state before the first change is preserved for rollback
        UNIT_ASSERT(rollback.Tables.contains(TableId));
        const auto& preserved = rollback.Tables.at(TableId);
        UNIT_ASSERT(preserved);
        UNIT_ASSERT(TCompare().Do(*preserved, before));
    }

    Y_UNIT_TEST(InvalidPolicy) {
        auto apply = [](const TTieringPolicy& policy) {
            auto alter = BuildTiered();
            alter.SetTieringPolicy(TableId, policy);
            Apply(alter);
        };

        // Missing column, unsupported type, missing unit for Uint32
        auto policy = RelativePolicy();
        policy.ColumnTag = 100;
        UNIT_ASSERT_EXCEPTION(apply(policy), yexception);
        policy.ColumnTag = TModel::ColId1;
        UNIT_ASSERT_EXCEPTION(apply(policy), yexception);
        policy.ColumnTag = TModel::ColId2;
        UNIT_ASSERT_EXCEPTION(apply(policy), yexception);
        policy.Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
        apply(policy);

        // Unknown family, the default tier, rules out of order, no rules
        policy = RelativePolicy();
        policy.Rules[0].TierId = ExtraTier;
        UNIT_ASSERT_EXCEPTION_CONTAINS(apply(policy), yexception, "is not a family of the table");
        policy = RelativePolicy();
        policy.Rules[0].TierId = TModel::GroupId1;
        UNIT_ASSERT_EXCEPTION_CONTAINS(apply(policy), yexception, "cannot target the default tier");
        policy = RelativePolicy();
        std::swap(policy.Rules[0], policy.Rules[1]);
        UNIT_ASSERT_EXCEPTION(apply(policy), yexception);
        policy = RelativePolicy();
        policy.Rules.clear();
        UNIT_ASSERT_EXCEPTION(apply(policy), yexception);
    }

    Y_UNIT_TEST(InvalidDropFamily) {
        auto drop = [](ui32 family) {
            TScheme scheme = Apply(BuildTiered());
            TAlter alter;
            alter.DropFamily(TableId, family);
            TSchemeModifier(scheme).Apply(*alter.Flush());
        };

        UNIT_ASSERT_EXCEPTION_CONTAINS(drop(TModel::GroupId1), yexception, "Cannot drop the leader family");
        UNIT_ASSERT_EXCEPTION_CONTAINS(drop(TModel::GroupId2), yexception, "Cannot drop family 3 of column 2");

        // Families without columns can be dropped
        drop(HddTier);
    }

    Y_UNIT_TEST(DropUsedTierOrColumn) {
        auto alter = BuildTiered();
        alter.SetTieringPolicy(TableId, RelativePolicy());

        {
            TScheme scheme = Apply(alter);
            TAlter drop;
            drop.DropFamily(TableId, HddTier);
            UNIT_ASSERT_EXCEPTION_CONTAINS(TSchemeModifier(scheme).Apply(*drop.Flush()), yexception,
                "Cannot drop family 1 used by the tiering policy as a tier");
        }

        {
            TScheme scheme = Apply(alter);
            TAlter drop;
            drop.DropColumn(TableId, TimeColumn);
            UNIT_ASSERT_EXCEPTION(TSchemeModifier(scheme).Apply(*drop.Flush()), yexception);
        }

        {
            // Other columns can be dropped
            TScheme scheme = Apply(alter);
            TAlter drop;
            drop.DropColumn(TableId, TModel::ColId2);
            UNIT_ASSERT(TSchemeModifier(scheme).Apply(*drop.Flush()));
        }
    }

    Y_UNIT_TEST(ConflictingChangesInOneRecord) {
        {
            TScheme scheme = Apply(BuildTiered());
            TAlter alter;
            alter.SetTieringPolicy(TableId, RelativePolicy());
            alter.ResetTieringPolicy(TableId);
            UNIT_ASSERT_EXCEPTION(TSchemeModifier(scheme).Apply(*AsOneRecord(alter)), yexception);
        }

        {
            // The policy cannot refer to a family that the record drops
            TScheme scheme = Apply(BuildTiered());
            TAlter alter;
            alter.SetTieringPolicy(TableId, RelativePolicy());
            alter.DropFamily(TableId, ArchiveTier);
            UNIT_ASSERT_EXCEPTION(TSchemeModifier(scheme).Apply(*AsOneRecord(alter)), yexception);
        }

        {
            // Families unused by the policy can be dropped in the same record
            auto build = BuildTiered();
            AddTier(build, ExtraTier);
            TScheme scheme = Apply(build);
            TAlter alter;
            alter.SetTieringPolicy(TableId, RelativePolicy());
            alter.DropFamily(TableId, ExtraTier);
            UNIT_ASSERT(TSchemeModifier(scheme).Apply(*AsOneRecord(alter)));
            UNIT_ASSERT(Table(scheme).TieringPolicy);
            UNIT_ASSERT(!Table(scheme).Families.contains(ExtraTier));
        }
    }

    Y_UNIT_TEST(RecordIsValidatedAsAWhole) {
        auto alter = BuildTiered();
        AddTier(alter, ExtraTier);
        alter.SetTieringPolicy(TableId, RelativePolicy());

        {
            // An invalid record changes nothing, even settings it lists before the tiering ones
            TScheme scheme = Apply(alter);
            TAlter change;
            change.SetColdBorrow(TableId, true);
            change.DropFamily(TableId, ExtraTier);
            change.DropFamily(TableId, HddTier);
            UNIT_ASSERT_EXCEPTION(TSchemeModifier(scheme).Apply(*AsOneRecord(change)), yexception);
            UNIT_ASSERT(!Table(scheme).ColdBorrow);
            UNIT_ASSERT_VALUES_EQUAL(Table(scheme).Families.size(), 5u);
        }

        {
            // Families are checked against the policy that the record leaves
            TScheme scheme = Apply(alter);
            TAlter change;
            change.ResetTieringPolicy(TableId);
            change.DropFamily(TableId, HddTier);
            UNIT_ASSERT(TSchemeModifier(scheme).Apply(*AsOneRecord(change)));
            UNIT_ASSERT(!Table(scheme).TieringPolicy);
            UNIT_ASSERT(!Table(scheme).Families.contains(HddTier));
        }
    }
}

}
}
