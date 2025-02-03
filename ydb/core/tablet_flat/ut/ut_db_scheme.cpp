#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_dbase_apply.h>

namespace NKikimr {
namespace NTable {

namespace {

    struct TModel {
        using ECodec = NKikimr::NTable::NPage::ECodec;
        using ECache = NKikimr::NTable::NPage::ECache;

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
            bld.SetFamily(TableId, GroupId1, ECache::Ever, ECodec::LZ4);
            bld.AddFamily(TableId, GroupId2, StoreIdOut);
            bld.AddColumn(TableId, "key", ColId1, NScheme::TSmallBoundedString::TypeId, false);
            bld.AddColumn(TableId, "value", ColId2, NScheme::TUint32::TypeId, false);
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
                && a.Large == b.Large;
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
                && ByValues(base.Rooms, with.Rooms);
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
}

}
}
