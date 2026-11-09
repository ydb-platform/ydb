#include "flat_comp_ut_common.h"

#include <ydb/core/tablet_flat/flat_comp_gen.h>
#include <ydb/core/tablet_flat/flat_compaction_prepare.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NTable {
namespace {

    constexpr ui32 TableId = 1;
    constexpr ui32 ColdTier = 42;
    constexpr ui32 ArchiveTier = 7;

    struct TFixture {
        NTest::TLayoutCook Layout;
        TScheme Scheme;
        TScheme::TTableInfo& Table;
        NLocalDb::TCompactionPolicyPtr Policy;
        TVector<TPartView> Parts;
        TVector<TIntrusiveConstPtr<TColdPart>> ColdParts;
        TCompactionState State;
        ui32 Serial = 1;
        const TInstant Now = TInstant::Seconds(1000);

        TFixture()
            : Table(Scheme.Tables.emplace(TableId, TScheme::TTableInfo("tiering", TableId)).first->second)
            , Policy(NLocalDb::CreateDefaultUserTablePolicy())
        {
            Layout.Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(0, 1, NScheme::NTypeIds::Uint64)
                .Key({0});
            const auto base = Policy->Generations.front();
            Policy->Generations.assign(5, base);
            for (auto& generation : Policy->Generations) {
                generation.SizeToCompact = ui64(1) << 60;
                generation.CountToCompact = 1000;
                generation.ForceSizeToCompact = ui64(1) << 61;
                generation.ForceCountToCompact = 1000;
                generation.UpliftPartSize = 0;
            }
            Policy->Generations[2].TierId = ColdTier;
            Policy->Generations[3].TierId = ColdTier;
            Policy->Generations[4].TierId = ArchiveTier;
            Table.CompactionPolicy = Policy;
            Table.Families[ColdTier].Room = ColdTier;
            Table.Rooms[ColdTier].Main = 2;
            Table.Rooms[ColdTier].Outer = 2;
            Table.Rooms[ColdTier].Blobs = {2};
            Table.Families[ArchiveTier].Room = ArchiveTier;
            Table.Rooms[ArchiveTier].Main = 3;
            Table.Rooms[ArchiveTier].Outer = 3;
            Table.Rooms[ArchiveTier].Blobs = {3};
            Table.TieringPolicy = TTieringPolicy{
                .ColumnTag = 1,
                .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
                .Rules = {
                    {.TierId = ArchiveTier, .Ranges = {{
                        .To = TTierRelativeBound{TDuration::Seconds(200)}, .ToInclusive = true}}},
                    {.TierId = ColdTier, .Ranges = {{
                        .To = TTierRelativeBound{TDuration::Seconds(100)}, .ToInclusive = true}}},
                },
            };
        }

        TPartView MakePart(i64 epoch, ui32 key = 1) {
            NPage::TConf conf;
            conf.Final = false;
            return NTest::TPartCook(Layout, conf,
                TLogoBlobID(123, 1, Serial++, 1, 0, 0), TEpoch::FromIndex(epoch))
                .AddN(key, ui64(100)).Finish().ToPartView();
        }

        TPartView AddPart(i64 epoch, ui32 level, ui32 key = 1) {
            auto part = MakePart(epoch, key);
            State.PartLevels[part->Label] = level;
            Parts.push_back(part);
            return part;
        }

        TIntrusiveConstPtr<TColdPart> AddColdPart(i64 epoch, ui32 level) {
            const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
                TLogoBlobID(123, 1, Serial++, 1, 0, 0), TEpoch::FromIndex(epoch));
            State.PartLevels[part->Label] = level;
            ColdParts.push_back(part);
            return part;
        }

        ui32 Tier(ui32 level) const {
            return level == 255 ? 0 : Policy->Generations.at(level - 1).TierId.value_or(0);
        }

        THolder<TCompactionParams> Params(ui32 sourceTier = 0) const {
            auto params = MakeHolder<TCompactionParams>();
            params->Table = TableId;
            params->TierId = sourceTier;
            params->Tiering = MakeHolder<TTieringCompaction>();
            params->Tiering->PartLevels = State.PartLevels;
            return params;
        }

        void Prepare(TCompactionParams& params, TSubset& subset, bool enabled = false) const {
            PrepareTieringCompaction(params, Table, Layout.RowScheme(), subset,
                Parts, ColdParts, enabled, Now);
        }

        bool Current(const TCompactionParams& params, bool enabled = false) const {
            return IsTieringCompactionCurrent(params, Table, Layout.RowScheme(),
                Parts, ColdParts, enabled);
        }
    };

    bool Contains(TConstArrayRef<TPartView> parts, const TLogoBlobID& label) {
        return std::any_of(parts.begin(), parts.end(), [&](const auto& part) {
            return part->Label == label;
        });
    }

    bool Contains(TConstArrayRef<TIntrusiveConstPtr<TColdPart>> parts, const TLogoBlobID& label) {
        return std::any_of(parts.begin(), parts.end(), [&](const auto& part) {
            return part->Label == label;
        });
    }

    class TControlledBroker final : public IResourceBroker {
    public:
        TTaskId SubmitTask(TString name, TResourceParams, TResourceConsumer consumer) override {
            const TTaskId id = NextId++;
            Pending.emplace(id, TTask{std::move(name), std::move(consumer)});
            return id;
        }

        void UpdateTask(TTaskId, TResourceParams) override { }

        void FinishTask(TTaskId id, EResourceStatus) override {
            Running.erase(id);
        }

        bool CancelTask(TTaskId id) override {
            return Pending.erase(id) != 0;
        }

        bool HasPending() const { return !Pending.empty(); }
        bool HasRunning() const { return !Running.empty(); }

        bool RunPending() {
            return !Pending.empty() && Grant(Pending.begin());
        }

        bool RunPending(ui32 generation) {
            const TString prefix = TStringBuilder() << "gen" << generation << "-table-";
            const auto it = std::find_if(Pending.begin(), Pending.end(), [&](const auto& entry) {
                return entry.second.Name.StartsWith(prefix);
            });
            return it != Pending.end() && Grant(it);
        }

    private:
        struct TTask {
            TString Name;
            TResourceConsumer Consumer;
        };

        bool Grant(TMap<TTaskId, TTask>::iterator it) {
            const TTaskId id = it->first;
            auto consumer = std::move(it->second.Consumer);
            Pending.erase(it);
            Running.insert(id);
            consumer(id);
            return true;
        }

        TTaskId NextId = 1;
        TMap<TTaskId, TTask> Pending;
        THashSet<TTaskId> Running;
    };

    // This backend exercises strategy metadata and lifecycle only. Output rows
    // are supplied by each test; real scan/router behavior has separate tests.
    class TMetadataBackend final : public ICompactionBackend {
    public:
        explicit TMetadataBackend(TFixture& fixture)
            : F(fixture)
        { }

        ui64 OwnerTabletId() const override { return 123; }
        const TScheme& DatabaseScheme() override { return F.Scheme; }
        TIntrusiveConstPtr<TRowScheme> RowScheme(ui32 table) const override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            return F.Layout.RowScheme();
        }
        const TScheme::TTableInfo* TableScheme(ui32 table) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            return &F.Table;
        }
        ui64 TableMemSize(ui32, TEpoch) override { return 0; }
        TPartView TablePart(ui32 table, const TLogoBlobID& label) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            for (const auto& part : F.Parts) {
                if (part->Label == label) {
                    return part;
                }
            }
            UNIT_FAIL("Unknown fixture part");
            return {};
        }
        TVector<TPartView> TableParts(ui32 table) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            return F.Parts;
        }
        TVector<TIntrusiveConstPtr<TColdPart>> TableColdParts(ui32 table) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            return F.ColdParts;
        }
        const TRowVersionRanges& TableRemovedRowVersions(ui32) override { return Removed; }
        bool IsDataTieringEnabled() const override { return Enabled; }
        void RequestMemCompactionRetry(ui32 table) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
            ++MemoryRetryRequests;
        }

        ui64 BeginCompaction(THolder<TCompactionParams> params) override {
            auto subset = MakeHolder<TSubset>(TEpoch::Zero(), F.Layout.RowScheme());
            subset->Flatten = params->Parts;
            subset->ColdParts = params->ColdParts;
            const ui32 generation = static_cast<const NCompGen::TGenCompactionParams&>(*params).Generation;
            if (generation == 0 && MemoryEpoch) {
                const auto memory = MakeIntrusive<TMemTable>(F.Layout.RowScheme(), *MemoryEpoch, ui64(0));
                subset->Frozen.emplace_back(memory, memory->Immediate());
            }
            UNIT_ASSERT(params->Tiering);
            PrepareTieringCompaction(*params, F.Table, F.Layout.RowScheme(), *subset,
                F.Parts, F.ColdParts, Enabled, F.Now);
            UNIT_ASSERT(Strategy);
            if (!Strategy->TryReserveCompaction(*params)) {
                Rejected.push_back(generation);
                return 0;
            }
            if (params->Mode == TCompactionParams::EMode::Regular) {
                params->Tiering->OutputTiers[100] = params->TierId;
            } else {
                for (ui32 index = 0; index < params->Tiering->TierOrder.size(); ++index) {
                    params->Tiering->OutputTiers[100 + index] = params->Tiering->TierOrder[index];
                }
            }
            const ui64 id = NextId++;
            Started.emplace(id, std::move(params));
            return id;
        }

        bool CancelCompaction(ui64 id) override {
            auto it = Started.find(id);
            if (it == Started.end()) {
                return false;
            }
            Cancelled.emplace(id, std::move(it->second));
            Started.erase(it);
            return true;
        }

        void RequestChanges(ui32 table) override {
            UNIT_ASSERT_VALUES_EQUAL(table, TableId);
        }

        TCompactionChanges Complete(NCompGen::TGenCompactionStrategy& strategy,
                TControlledBroker& broker, ui64 id, TVector<ui32> outputTiers) {
            auto it = Started.find(id);
            UNIT_ASSERT(it != Started.end());
            auto params = std::move(it->second);
            Started.erase(it);
            UNIT_ASSERT(IsTieringCompactionCurrent(*params, F.Table, F.Layout.RowScheme(),
                F.Parts, F.ColdParts, Enabled));
            const TEpoch epoch = params->Context->Epochs
                ? params->Context->Epochs->Max : TEpoch::Min();
            auto result = MakeHolder<TCompactionResult>(epoch, outputTiers.size());
            ui32 key = 100;
            for (ui32 tier : outputTiers) {
                auto output = F.MakePart(epoch.ToProto(), key++);
                const auto mapping = std::find_if(params->Tiering->OutputTiers.begin(),
                    params->Tiering->OutputTiers.end(), [tier](const auto& entry) {
                        return entry.second == tier;
                    });
                UNIT_ASSERT(mapping != params->Tiering->OutputTiers.end());
                result->PartOutputIds[output->Label] = mapping->first;
                result->Parts.push_back(output);
            }

            // Production replaces the subset before notifying the strategy.
            THashSet<TLogoBlobID> removed;
            for (const auto& part : params->Context->Inputs) {
                removed.insert(part.Label());
            }
            F.Parts.erase(std::remove_if(F.Parts.begin(), F.Parts.end(),
                [&](const auto& part) { return removed.contains(part->Label); }), F.Parts.end());
            F.ColdParts.erase(std::remove_if(F.ColdParts.begin(), F.ColdParts.end(),
                [&](const auto& part) { return removed.contains(part->Label); }), F.ColdParts.end());
            F.Parts.insert(F.Parts.end(), result->Parts.begin(), result->Parts.end());
            if (params->TaskId) {
                broker.FinishTask(params->TaskId, EResourceStatus::Finished);
            }
            auto changes = strategy.CompactionFinished(id, std::move(params), std::move(result));
            F.State = strategy.SnapshotState();
            return changes;
        }

        ui64 OnlyStarted() const {
            UNIT_ASSERT_VALUES_EQUAL(Started.size(), 1);
            return Started.begin()->first;
        }

        ui64 StartedGeneration(ui32 generation) const {
            const auto it = std::find_if(Started.begin(), Started.end(), [&](const auto& entry) {
                return static_cast<const NCompGen::TGenCompactionParams&>(*entry.second).Generation == generation;
            });
            UNIT_ASSERT(it != Started.end());
            return it->first;
        }

        TFixture& F;
        bool Enabled = false;
        NCompGen::TGenCompactionStrategy* Strategy = nullptr;
        std::optional<TEpoch> MemoryEpoch;
        TVector<ui32> Rejected;
        ui32 MemoryRetryRequests = 0;
        THashMap<ui64, THolder<TCompactionParams>> Started;
        THashMap<ui64, THolder<TCompactionParams>> Cancelled;

    private:
        ui64 NextId = 1;
        TRowVersionRanges Removed;
    };

    struct TStrategyFixture : TFixture {
        TMetadataBackend Backend{*this};
        TControlledBroker Broker;
        NTest::TSimpleLogger Logger;
        NTest::TSimpleTime Time;
        THolder<NCompGen::TGenCompactionStrategy> Strategy;

        ~TStrategyFixture() {
            if (Strategy) {
                Strategy->Stop();
            }
        }

        void Start() {
            Time.Move(Now);
            Strategy = MakeHolder<NCompGen::TGenCompactionStrategy>(TableId,
                &Backend, &Broker, &Time, &Logger, "tiering-test");
            Backend.Strategy = Strategy.Get();
            Strategy->Start(State);
        }

        ui64 RunPending() {
            for (ui32 step = 0; Backend.Started.empty() && Broker.HasPending() && step < 20; ++step) {
                UNIT_ASSERT(Broker.RunPending());
            }
            return Backend.OnlyStarted();
        }

        TCompactionChanges Complete(ui64 id, TVector<ui32> tiers) {
            return Backend.Complete(*Strategy, Broker, id, std::move(tiers));
        }

        void Cancel(ui64 id) {
            UNIT_ASSERT(Backend.CancelCompaction(id));
            const auto& cancelled = *Backend.Cancelled.at(id);
            if (cancelled.TaskId) {
                Broker.FinishTask(cancelled.TaskId, EResourceStatus::Cancelled);
            }
            Strategy->CompactionCancelled(id, cancelled);
        }
    };

}

Y_UNIT_TEST_SUITE(TTieringCompactionPreparation) {

    Y_UNIT_TEST(ConflictLimitDefaultsToFiveAndRoundTripsThroughThePolicy) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.Policy->TieringCompactionConflictLimit, 5);
        f.Policy->TieringCompactionConflictLimit = 9;
        NKikimrCompaction::TCompactionPolicy proto;
        f.Policy->Serialize(proto);
        const NLocalDb::TCompactionPolicy restored(proto);
        UNIT_ASSERT_VALUES_EQUAL(restored.TieringCompactionConflictLimit, 9);

        proto.ClearTieringCompactionConflictLimit();
        const NLocalDb::TCompactionPolicy legacy(proto);
        UNIT_ASSERT_VALUES_EQUAL(legacy.TieringCompactionConflictLimit, 5);
    }

    Y_UNIT_TEST(ZeroConflictLimitIsRejectedByPolicyValidation) {
        TFixture f;
        TString error;
        UNIT_ASSERT_C(NLocalDb::ValidateCompactionPolicy(*f.Policy, error), error);
        f.Policy->TieringCompactionConflictLimit = 0;
        UNIT_ASSERT(!NLocalDb::ValidateCompactionPolicy(*f.Policy, error));
        UNIT_ASSERT(error.Contains("TieringCompactionConflictLimit"));
    }

    Y_UNIT_TEST(ClosesAColdIntervalAcrossHotSourcesWithTheFlagOff) {
        TFixture f;
        auto coldOld = f.AddPart(10, 4);
        auto hot = f.AddPart(20, 2);
        auto coldNew = f.AddPart(30, 3, 2);
        auto outside = f.AddPart(40, 1, 3);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {coldOld, coldNew};
        f.Prepare(*params, subset);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
        UNIT_ASSERT_VALUES_EQUAL(subset.Flatten.size(), 3);
        UNIT_ASSERT(Contains(subset.Flatten, hot->Label));
        UNIT_ASSERT(!Contains(subset.Flatten, outside->Label));
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Epochs->Max, TEpoch::FromIndex(30));
        params->Context->ValidateSubset(subset);
        UNIT_ASSERT(f.Current(*params));
    }

    Y_UNIT_TEST(IncludesEqualEpochPeersAcrossTiers) {
        TFixture f;
        auto coldOld = f.AddPart(10, 4);
        auto lowerPeer = f.AddPart(10, 2, 2);
        auto coldNew = f.AddPart(30, 3, 3);
        auto upperPeer = f.AddPart(30, 1, 4);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {coldOld, coldNew};
        f.Prepare(*params, subset);

        UNIT_ASSERT_VALUES_EQUAL(subset.Flatten.size(), 4);
        UNIT_ASSERT(Contains(subset.Flatten, lowerPeer->Label));
        UNIT_ASSERT(Contains(subset.Flatten, upperPeer->Label));
        UNIT_ASSERT(params->Context->Remaining.empty());
    }

    Y_UNIT_TEST(ClosesTheIntervalOverUnloadedPartsAndEqualEpochPeers) {
        TFixture f;
        auto coldOld = f.AddPart(10, 4);
        auto lowerPeer = f.AddColdPart(10, 4);
        auto hot = f.AddColdPart(20, 2);
        auto coldNew = f.AddPart(30, 3, 2);
        auto outside = f.AddColdPart(40, 1);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {coldOld, coldNew};
        f.Prepare(*params, subset);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
        UNIT_ASSERT_VALUES_EQUAL(subset.Flatten.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(subset.ColdParts.size(), 2);
        UNIT_ASSERT(Contains(subset.ColdParts, lowerPeer->Label));
        UNIT_ASSERT(Contains(subset.ColdParts, hot->Label));
        UNIT_ASSERT(!Contains(subset.ColdParts, outside->Label));
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Remaining.front().Level, 1);
        params->Context->ValidateSubset(subset);
        UNIT_ASSERT(f.Current(*params));
    }

    Y_UNIT_TEST(SingleColdTierKeepsRegularPlacementWithoutCooling) {
        TFixture f;
        auto cold = f.AddPart(10, 3);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {cold};
        f.Prepare(*params, subset);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(params->TierId, ColdTier);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
    }

    Y_UNIT_TEST(EnabledPolicyStartsAnExplicitSingleTierTac) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        params->Mode = TCompactionParams::EMode::TierAware;
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset, true);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(params->Tiering->AllowCooling);
        UNIT_ASSERT_VALUES_EQUAL(params->Tiering->Now, f.Now);
        UNIT_ASSERT(params->Tiering->Schema->TieringPolicy == f.Table.TieringPolicy);
    }

    Y_UNIT_TEST(RegularCompactionDoesNotEnableCoolingWithTheFlagAndPolicy) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset, true);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
        UNIT_ASSERT(f.Current(*params, false));
    }

    Y_UNIT_TEST(MixedInputsUpgradeRegularToMaintenanceTacWithoutEnablingCooling) {
        TFixture f;
        auto coldOld = f.AddPart(10, 4);
        f.AddPart(20, 2, 2);
        auto coldNew = f.AddPart(30, 3, 3);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {coldOld, coldNew};
        f.Prepare(*params, subset, true);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Inputs.size(), 3);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
        UNIT_ASSERT(f.Current(*params, false));
    }

    Y_UNIT_TEST(AFlagWithoutAPolicyDoesNotEnableCooling) {
        TFixture f;
        f.Table.TieringPolicy.reset();
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset, true);

        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT(!params->Tiering->AllowCooling);
    }

    Y_UNIT_TEST(ActualFrozenAndStatusEpochsExtendThePreparedInputs) {
        TFixture f;
        auto cold = f.AddPart(10, 3);
        auto hot = f.AddPart(20, 2, 2);
        auto archive = f.AddPart(30, 5, 3);
        auto memory = MakeIntrusive<TMemTable>(f.Layout.RowScheme(), TEpoch::FromIndex(40), ui64(0));
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {cold};
        subset.Frozen.emplace_back(memory, memory->Immediate());
        subset.TxStatus.emplace_back(new TTxStatusPart(TLogoBlobID(123, 1, 99, 1, 0, 0),
            TEpoch::FromIndex(45), nullptr));
        auto params = f.Params();
        f.Prepare(*params, subset);

        UNIT_ASSERT_VALUES_EQUAL(subset.Flatten.size(), 3);
        UNIT_ASSERT(Contains(subset.Flatten, hot->Label));
        UNIT_ASSERT(Contains(subset.Flatten, archive->Label));
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Epochs->Max, TEpoch::FromIndex(45));
        UNIT_ASSERT(params->Context->RequiresTierAware());
        params->Context->ValidateSubset(subset);
    }

    Y_UNIT_TEST(DisablingTheFlagInvalidatesANewCoolingOperation) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        params->Mode = TCompactionParams::EMode::TierAware;
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset, true);

        UNIT_ASSERT(f.Current(*params, true));
        UNIT_ASSERT(!f.Current(*params, false));
    }

    Y_UNIT_TEST(StorageChangesInvalidateBeforePublication) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);

        f.Table.Rooms[ColdTier].Main = 9;
        UNIT_ASSERT(!f.Current(*params));
        f.Table.Rooms[ColdTier].Main = 2;
        UNIT_ASSERT(f.Current(*params));
        f.Table.Families[ColdTier].Small = 32;
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(PolicyChangesInvalidateBeforePublication) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);

        f.Table.TieringPolicy->Rules[0].Ranges[0].To = TTierRelativeBound{TDuration::Seconds(300)};
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(CompactionPolicySnapshotDoesNotFollowMutableSettings) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);

        ++f.Policy->Generations.front().ForceCountToCompact;
        UNIT_ASSERT(!f.Current(*params));
        --f.Policy->Generations.front().ForceCountToCompact;
        UNIT_ASSERT(f.Current(*params));
        f.Policy->Generations[2].TierId = 0;
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(ChangedInventoryInvalidatesThePinnedContext) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);

        f.AddPart(20, 1, 2);
        UNIT_ASSERT(!f.Current(*params));
        f.Parts.pop_back();
        UNIT_ASSERT(f.Current(*params));
        f.Parts.front() = hot.CloneWithEpoch(TEpoch::FromIndex(11));
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(ReplacingAnUnloadedPartInvalidatesThePinnedContext) {
        TFixture f;
        auto cold = f.AddColdPart(10, 3);
        auto params = f.Params(ColdTier);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.ColdParts = {cold};
        f.Prepare(*params, subset);
        UNIT_ASSERT(params->Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(params->TierId, 0);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->InputTiers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->InputTiers.front(), 0);
        UNIT_ASSERT(f.Current(*params));

        f.ColdParts.front() = new TColdPart(cold->Label, cold->Epoch);
        UNIT_ASSERT(!f.Current(*params));
        f.ColdParts.front() = cold;
        UNIT_ASSERT(f.Current(*params));
    }

    Y_UNIT_TEST(AKnownRemainingReplacementPreservesThePinnedMaskContext) {
        TFixture f;
        const auto hot = f.AddPart(40, 1);
        const auto oldCold = f.AddPart(10, 3, 2);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);
        params->Tiering->UseConservativeMasks = true;

        const auto archive = f.MakePart(10, 2);
        f.Parts.back() = archive;
        f.State.PartLevels.erase(oldCold->Label);
        f.State.PartLevels[archive->Label] = 5;
        UNIT_ASSERT(!f.Current(*params));

        params->Tiering->ExpectedInventory = DescribeCompactionParts(
            f.Parts, f.ColdParts, f.State, *f.Policy);
        params->Tiering->PartLevels = f.State.PartLevels;
        UNIT_ASSERT(f.Current(*params));
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Remaining.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Remaining.front().Label(), oldCold->Label);
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Remaining.front().TierId, ColdTier);
    }

    Y_UNIT_TEST(AKnownInventoryUpdateCannotAddAPartInsideTheClosedEpochRange) {
        TFixture f;
        const auto hot = f.AddPart(40, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);
        params->Tiering->UseConservativeMasks = true;

        f.AddPart(40, 3, 2);
        params->Tiering->ExpectedInventory = DescribeCompactionParts(
            f.Parts, f.ColdParts, f.State, *f.Policy);
        params->Tiering->PartLevels = f.State.PartLevels;
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(AKnownInventoryUpdateCannotReplaceAnOriginalInput) {
        TFixture f;
        const auto hot = f.AddPart(40, 1);
        auto params = f.Params();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Prepare(*params, subset);
        params->Tiering->UseConservativeMasks = true;

        f.Parts.front() = hot.CloneWithEpoch(TEpoch::FromIndex(40));
        params->Tiering->ExpectedInventory = DescribeCompactionParts(
            f.Parts, f.ColdParts, f.State, *f.Policy);
        UNIT_ASSERT(!f.Current(*params));
    }

    Y_UNIT_TEST(PreferredFinalPlacementChecksUnloadedUpperGenerations) {
        TFixture f;
        f.AddPart(40, 1);
        f.AddColdPart(20, 2);
        f.AddPart(9, 255, 2);
        const auto remaining = DescribeCompactionParts(f.Parts, f.ColdParts, f.State, *f.Policy);
        const auto level = FindTierCompactionLevel(*f.Policy, remaining, 0,
            TEpoch::FromIndex(30), 255);

        UNIT_ASSERT(level);
        UNIT_ASSERT_VALUES_EQUAL(*level, 1);
    }

    Y_UNIT_TEST(ImpossibleWarmPlacementExpandsToTheFullInventoryBeforeScanning) {
        TFixture f;
        auto upper = f.AddPart(40, 1);
        auto overlap = f.AddPart(5, 2, 2);
        auto final = f.AddPart(9, 255, 3);
        auto cold = f.AddPart(7, 3, 4);
        auto archive = f.AddColdPart(50, 5);
        auto params = f.Params(ColdTier);
        params->Mode = TCompactionParams::EMode::TierAware;
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {cold};
        const auto initial = TCompactionContext::Build(
            DescribeCompactionParts(f.Parts, f.ColdParts, f.State, *f.Policy), subset);
        UNIT_ASSERT(!FindTierCompactionLevel(*f.Policy, initial->Remaining, 0,
            TEpoch::FromIndex(7)));
        f.Prepare(*params, subset, true);

        UNIT_ASSERT_VALUES_EQUAL(subset.Flatten.size(), 4);
        UNIT_ASSERT(Contains(subset.Flatten, upper->Label));
        UNIT_ASSERT(Contains(subset.Flatten, overlap->Label));
        UNIT_ASSERT(Contains(subset.Flatten, final->Label));
        UNIT_ASSERT_VALUES_EQUAL(subset.ColdParts.size(), 1);
        UNIT_ASSERT(Contains(subset.ColdParts, archive->Label));
        UNIT_ASSERT(params->Context->Remaining.empty());
        UNIT_ASSERT_VALUES_EQUAL(params->Context->Epochs->Max, TEpoch::FromIndex(50));
        params->Context->ValidateSubset(subset);
        UNIT_ASSERT(f.Current(*params, true));
    }

    Y_UNIT_TEST(PolicyTargetsRequireFamiliesAndGenerationBindings) {
        TFixture f;
        auto hot = f.AddPart(10, 1);
        auto params = f.Params();
        params->Mode = TCompactionParams::EMode::TierAware;
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        f.Table.Families.erase(ArchiveTier);
        UNIT_ASSERT_EXCEPTION(f.Prepare(*params, subset, true), yexception);
        f.Table.Families[ArchiveTier].Room = ArchiveTier;
        f.Policy->Generations.back().TierId = ColdTier;
        UNIT_ASSERT_EXCEPTION(f.Prepare(*params, subset, true), yexception);
    }

}

Y_UNIT_TEST_SUITE(TTieringCompactionStrategy) {

    Y_UNIT_TEST(RestoresOverlappingEpochsInDifferentTiers) {
        TStrategyFixture f;
        auto hot = f.AddPart(20, 1);
        auto cold = f.AddPart(30, 3, 2);
        auto archive = f.AddPart(40, 5, 3);
        f.Start();

        const auto state = f.Strategy->SnapshotState();
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(hot->Label), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(cold->Label), 3);
        UNIT_ASSERT_VALUES_EQUAL(state.PartLevels.at(archive->Label), 5);
        UNIT_ASSERT(f.Backend.Started.empty());
    }

    Y_UNIT_TEST(OrdinaryCompactionDoesNotTakeTheNextTierOrPlaceResultsThere) {
        TStrategyFixture f;
        auto hotOld = f.AddPart(10, 2);
        auto hotNew = f.AddPart(20, 2, 2);
        auto cold = f.AddPart(30, 3, 3);
        f.Policy->Generations[1].ForceCountToCompact = 2;
        f.Start();
        const auto id = f.RunPending();
        const auto& params = *f.Backend.Started.at(id);

        UNIT_ASSERT(params.Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(params.TierId, 0);
        UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.size(), 2);
        UNIT_ASSERT(Contains(params.Parts, hotOld->Label));
        UNIT_ASSERT(Contains(params.Parts, hotNew->Label));
        UNIT_ASSERT(!Contains(params.Parts, cold->Label));
        const auto changes = f.Complete(id, {0});
        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
        for (const auto& [label, level] : changes.NewPartLevels) {
            UNIT_ASSERT_VALUES_EQUAL(f.Tier(level), 0);
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(label), level);
        }
    }

    Y_UNIT_TEST(ColdCompactionExpandsToMaintenanceTacWhenTheFlagIsOff) {
        TStrategyFixture f;
        auto old = f.AddPart(10, 3);
        auto hot = f.AddPart(20, 2, 2);
        auto newer = f.AddPart(30, 3, 3);
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();
        const auto id = f.RunPending();
        const auto& params = *f.Backend.Started.at(id);

        UNIT_ASSERT(params.Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(!params.Tiering->AllowCooling);
        UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.size(), 3);
        UNIT_ASSERT(Contains(params.Parts, old->Label));
        UNIT_ASSERT(Contains(params.Parts, hot->Label));
        UNIT_ASSERT(Contains(params.Parts, newer->Label));
    }

    Y_UNIT_TEST(EachOutputKeepsItsOwnGenerationAfterRestart) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        f.AddPart(90, 1);
        f.AddPart(100, 1, 2);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Start();
        const auto id = f.RunPending();
        UNIT_ASSERT(f.Backend.Started.at(id)->Mode == TCompactionParams::EMode::TierAware);
        const auto changes = f.Complete(id, {0, ColdTier, ArchiveTier});

        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 3);
        THashSet<ui32> tiers;
        for (const auto& [label, level] : changes.NewPartLevels) {
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(label), level);
            tiers.insert(f.Tier(level));
        }
        UNIT_ASSERT(tiers.contains(0));
        UNIT_ASSERT(tiers.contains(ColdTier));
        UNIT_ASSERT(tiers.contains(ArchiveTier));
        const auto before = f.State.PartLevels;
        f.Strategy->Stop();
        f.Strategy->Start(f.State);
        UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == before);
    }

    Y_UNIT_TEST(OldWarmOutputIsPlacedBelowNewHotEpochs) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        auto hotNew = f.AddPart(40, 1);
        auto hotOld = f.AddPart(20, 2, 2);
        f.AddPart(10, 3, 3);
        f.AddPart(9, 3, 4);
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();
        const auto id = f.RunPending();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(id)->Context->Epochs->Max,
            TEpoch::FromIndex(10));
        const auto changes = f.Complete(id, {0});

        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
        const ui32 level = changes.NewPartLevels.begin()->second;
        UNIT_ASSERT_VALUES_EQUAL(level, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(hotNew->Label), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(hotOld->Label), 2);
        f.Strategy->Stop();
        f.Strategy->Start(f.State);
    }

    Y_UNIT_TEST(WarmOutputNewerThanFinalPartsStaysInTheLastHotGeneration) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        f.AddPart(40, 1);
        f.AddPart(20, 2, 2);
        auto final = f.AddPart(9, 255, 3);
        f.AddPart(10, 3, 4);
        f.Policy->Generations[2].ForceCountToCompact = 1;
        f.Start();
        const auto id = f.RunPending();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(id)->Context->Inputs.size(), 1);
        const auto changes = f.Complete(id, {0});

        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(final->Label), 255);
        f.Strategy->Stop();
        f.Strategy->Start(f.State);
    }

    Y_UNIT_TEST(WarmOutputOlderThanFinalPartsIsPlacedInFinalParts) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        f.AddPart(40, 1);
        f.AddPart(20, 2, 2);
        auto final = f.AddPart(9, 255, 3);
        f.AddPart(0, 3, 4);
        f.Policy->Generations[2].ForceCountToCompact = 1;
        f.Start();
        const auto id = f.RunPending();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(id)->Context->Inputs.size(), 1);
        const auto changes = f.Complete(id, {0});

        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, 255);
        UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(final->Label), 255);
        f.Strategy->Stop();
        f.Strategy->Start(f.State);
    }

    Y_UNIT_TEST(UnloadedPartsRestoreAndRewriteIntoDefaultDespiteSavedColdGenerations) {
        for (ui32 savedLevel : {3, 4, 5}) {
            TStrategyFixture f;
            const auto hot = f.AddPart(20, 1);
            const auto loadedCold = f.AddPart(30, 3, 2);
            const auto unloaded = f.AddColdPart(10, savedLevel);
            f.Start();

            // Loaded parts retain their tier. Only the unloaded source's stale
            // non-default placement is normalized, before it can start work.
            f.State = f.Strategy->SnapshotState();
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(unloaded->Label), 255);
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(hot->Label), 1);
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(loadedCold->Label), 3);
            const auto restored = f.State.PartLevels;
            f.Strategy->Stop();
            f.Strategy->Start(f.State);
            UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == restored);

            const auto id = f.RunPending();
            const auto& params = *f.Backend.Started.at(id);
            UNIT_ASSERT(params.Mode == TCompactionParams::EMode::Regular);
            UNIT_ASSERT_VALUES_EQUAL(params.TierId, 0);
            UNIT_ASSERT(!params.Tiering->AllowCooling);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<const NCompGen::TGenCompactionParams&>(params).Generation, 255);
            UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.front().Level, 255);
            UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.front().TierId, 0);
            UNIT_ASSERT_VALUES_EQUAL(params.ColdParts.size(), 1);
            UNIT_ASSERT(Contains(params.ColdParts, unloaded->Label));
            UNIT_ASSERT(params.Parts.empty());
            const auto changes = f.Complete(id, {0});

            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, 255);
            UNIT_ASSERT(!f.State.PartLevels.contains(unloaded->Label));
            UNIT_ASSERT(f.ColdParts.empty());
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(hot->Label), 1);
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(loadedCold->Label), 3);
            const auto before = f.State.PartLevels;
            f.Strategy->Stop();
            f.Strategy->Start(f.State);
            UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == before);
        }
    }

    Y_UNIT_TEST(UnloadedPartsKeepTheirDefaultGenerationThroughRestartAndRewrite) {
        for (ui32 level : {1, 2}) {
            TStrategyFixture f;
            f.AddPart(20, 1);
            const auto unloaded = f.AddColdPart(10, level);
            f.Start();
            f.State = f.Strategy->SnapshotState();
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(unloaded->Label), level);
            const auto restored = f.State.PartLevels;
            f.Strategy->Stop();
            f.Strategy->Start(f.State);
            UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == restored);

            const auto id = f.RunPending();
            const auto& params = *f.Backend.Started.at(id);
            UNIT_ASSERT(params.Mode == TCompactionParams::EMode::Regular);
            UNIT_ASSERT_VALUES_EQUAL(params.TierId, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<const NCompGen::TGenCompactionParams&>(params).Generation, level);
            UNIT_ASSERT(Contains(params.ColdParts, unloaded->Label));
            const auto changes = f.Complete(id, {0});
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, level == 1 ? 2 : 255);
            UNIT_ASSERT(!f.State.PartLevels.contains(unloaded->Label));
            UNIT_ASSERT(f.ColdParts.empty());
            const auto before = f.State.PartLevels;
            f.Strategy->Stop();
            f.Strategy->Start(f.State);
            UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == before);
        }
    }

    Y_UNIT_TEST(UnloadedDefaultGenerationCannotBeRetaggedAsAColdTier) {
        TStrategyFixture f;
        const auto unloaded = f.AddColdPart(10, 2);
        f.Start();
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->SnapshotState().PartLevels.at(unloaded->Label), 2);

        // ReflectSchema must compare distinct before/after policies; mutating
        // the existing policy would also change the strategy's old settings.
        NKikimrCompaction::TCompactionPolicy serialized;
        f.Policy->Serialize(serialized);
        NLocalDb::TCompactionPolicyPtr updated = new NLocalDb::TCompactionPolicy(serialized);
        updated->Generations[1].TierId = ColdTier;
        f.Table.CompactionPolicy = updated;
        UNIT_ASSERT_EXCEPTION(f.Strategy->ReflectSchema(), yexception);
        f.Table.CompactionPolicy = f.Policy;
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->SnapshotState().PartLevels.at(unloaded->Label), 2);
    }

    Y_UNIT_TEST(UnloadedFinalPartCompactsIntoTheDefaultTierDespiteColdLastGenerations) {
        TStrategyFixture f;
        auto hot = f.AddPart(20, 2);
        auto loaded = f.AddPart(10, 255, 2);
        auto unloaded = f.AddColdPart(9, 255);
        f.Start();
        const auto id = f.RunPending();
        const auto& params = *f.Backend.Started.at(id);
        UNIT_ASSERT(params.Mode == TCompactionParams::EMode::Regular);
        UNIT_ASSERT_VALUES_EQUAL(params.TierId, 0);
        UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.size(), 2);
        UNIT_ASSERT(Contains(params.Parts, loaded->Label));
        UNIT_ASSERT(Contains(params.ColdParts, unloaded->Label));
        const auto changes = f.Complete(id, {0});

        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, 255);
        UNIT_ASSERT(!f.State.PartLevels.contains(loaded->Label));
        UNIT_ASSERT(!f.State.PartLevels.contains(unloaded->Label));
        UNIT_ASSERT(f.ColdParts.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(hot->Label), 2);
        const auto before = f.State.PartLevels;
        f.Strategy->Stop();
        f.Strategy->Start(f.State);
        UNIT_ASSERT(f.Strategy->SnapshotState().PartLevels == before);
    }

    Y_UNIT_TEST(UpliftRaisesOutputsWithinTheirTierAndStopsAtTheTierBoundary) {
        for (ui32 source : {1, 3}) {
            TStrategyFixture f;
            for (auto& generation : f.Policy->Generations) {
                generation.UpliftPartSize = Max<ui64>();
            }
            f.AddPart(10, source);
            f.AddPart(20, source, 2);
            const ui32 foreignLevel = source == 1 ? 3 : 5;
            auto foreign = f.AddPart(30, foreignLevel, 3);
            f.Policy->Generations[source - 1].ForceCountToCompact = 2;
            f.Start();
            const auto id = f.RunPending();
            const auto& params = *f.Backend.Started.at(id);
            UNIT_ASSERT(params.Mode == TCompactionParams::EMode::Regular);
            UNIT_ASSERT_VALUES_EQUAL(params.Context->Inputs.size(), 2);
            UNIT_ASSERT(!Contains(params.Parts, foreign->Label));
            const auto changes = f.Complete(id, {f.Tier(source)});

            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.NewPartLevels.begin()->second, source);
            UNIT_ASSERT_VALUES_EQUAL(f.State.PartLevels.at(foreign->Label), foreignLevel);
        }
    }

    Y_UNIT_TEST(MemoryCompactionRunsBesideADisjointDiskCompaction) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        auto first = f.AddPart(10, 3);
        auto second = f.AddPart(20, 3, 2);
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Backend.MemoryEpoch = TEpoch::FromIndex(30);
        f.Start();
        const auto diskId = f.RunPending();
        const ui64 memoryId = f.Strategy->BeginMemCompaction(0, {0, TEpoch::Max()}, 0);

        UNIT_ASSERT(memoryId != 0);
        UNIT_ASSERT(memoryId != diskId);
        UNIT_ASSERT(f.Backend.Cancelled.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.size(), 2);
        UNIT_ASSERT(f.Backend.Started.at(memoryId)->Context->Inputs.empty());
        UNIT_ASSERT(f.Backend.Started.at(memoryId)->Tiering->UseConservativeMasks);
        UNIT_ASSERT(!f.Backend.Started.at(memoryId)->Tiering->AllowCooling);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(memoryId)->Context->Epochs->Max,
            TEpoch::FromIndex(30));
        f.Complete(memoryId, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), diskId);
        UNIT_ASSERT(f.State.PartLevels.contains(first->Label));
        UNIT_ASSERT(f.State.PartLevels.contains(second->Label));
        f.Complete(diskId, {ColdTier});
    }

    Y_UNIT_TEST(DisjointGenerationsCanCompactConcurrently) {
        TStrategyFixture f;
        f.AddPart(40, 1, 100);
        f.AddPart(41, 1, 101);
        f.AddPart(10, 3, 100);
        f.AddPart(11, 3, 101);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();

        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 coldId = f.Backend.StartedGeneration(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.size(), 2);
        UNIT_ASSERT(f.Backend.Cancelled.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(hotId)->Context->Inputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(coldId)->Context->Inputs.size(), 2);

        f.Complete(coldId, {ColdTier});
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        UNIT_ASSERT(f.Current(*f.Backend.Started.at(hotId)));
        const auto external = f.AddPart(100, 1, 999);
        UNIT_ASSERT(!f.Current(*f.Backend.Started.at(hotId)));
        f.Parts.pop_back();
        f.State.PartLevels.erase(external->Label);
        UNIT_ASSERT(f.Current(*f.Backend.Started.at(hotId)));
        f.Complete(hotId, {0});
        UNIT_ASSERT(f.Backend.Started.empty());
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
    }

    Y_UNIT_TEST(AParallelDiskTacTreatsMemoryOutputsAsUnknownRemainingKeys) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        f.Backend.MemoryEpoch = TEpoch::FromIndex(40);
        f.AddPart(10, 3, 100);
        f.AddPart(20, 3, 101);
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();
        const ui64 memoryId = f.Strategy->BeginMemCompaction(0, {0, TEpoch::Max()}, 0);
        UNIT_ASSERT(memoryId != 0);
        UNIT_ASSERT(f.Backend.Started.at(memoryId)->Tiering->HasMemoryInputs);

        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 diskId = f.Backend.StartedGeneration(3);
        const auto& disk = *f.Backend.Started.at(diskId);
        UNIT_ASSERT(disk.Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(disk.Tiering->UseConservativeMasks);
        UNIT_ASSERT(disk.Tiering->HasUnknownRemaining);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.size(), 2);

        f.Complete(diskId, {ColdTier});
        UNIT_ASSERT(f.Current(*f.Backend.Started.at(memoryId), true));
        f.Complete(memoryId, {});
    }

    Y_UNIT_TEST(ALateCancellationDoesNotReleaseTheReplacementOrAnotherGeneration) {
        TStrategyFixture f;
        f.AddPart(40, 1, 100);
        f.AddPart(41, 1, 101);
        f.AddPart(10, 3, 100);
        f.AddPart(11, 3, 101);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();
        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 originalId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 coldId = f.Backend.StartedGeneration(3);
        f.Cancel(originalId);

        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), coldId);
        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 retryId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(retryId != originalId);
        f.Strategy->CompactionCancelled(originalId, *f.Backend.Cancelled.at(originalId));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.StartedGeneration(1), retryId);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.StartedGeneration(3), coldId);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(1), 0);
        f.Complete(coldId, {ColdTier});
        f.Complete(retryId, {0});
    }

    Y_UNIT_TEST(BusyExtraInputsCancelOnlyTheRequestingGenerationUntilOccupancyChanges) {
        TStrategyFixture f;
        f.AddPart(40, 1);
        f.AddPart(41, 1, 2);
        f.AddPart(10, 3, 3);
        f.AddPart(50, 3, 4);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();

        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Rejected.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Rejected.back(), 3);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT(!f.Broker.RunPending(3));
        f.Strategy->UpdateCompactions();
        UNIT_ASSERT(!f.Broker.RunPending(3));
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        UNIT_ASSERT(f.Backend.Cancelled.empty());

        f.Complete(hotId, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 coldId = f.Backend.StartedGeneration(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(coldId)->Context->Inputs.size(), 2);
        UNIT_ASSERT(f.Backend.Started.at(coldId)->Mode == TCompactionParams::EMode::Regular);
        f.Complete(coldId, {ColdTier});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
    }

    Y_UNIT_TEST(RegularSuccessRetainsConflictsUntilTheSameGenerationCompletesATac) {
        TStrategyFixture f;
        f.AddPart(40, 1);
        f.AddPart(41, 1, 2);
        f.AddPart(10, 3, 3);
        f.AddPart(50, 3, 4);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        // Keep the regular result in its source generation so a subsequent
        // policy sweep exercises the same generation's accumulated conflicts.
        f.Policy->Generations[2].UpliftPartSize = Max<ui64>();
        f.Start();

        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        f.Complete(hotId, {});

        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 regularId = f.Backend.OnlyStarted();
        UNIT_ASSERT(f.Backend.Started.at(regularId)->Mode == TCompactionParams::EMode::Regular);
        const auto regularChanges = f.Complete(regularId, {ColdTier});
        UNIT_ASSERT_VALUES_EQUAL(regularChanges.NewPartLevels.begin()->second, 3);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());

        f.Backend.Enabled = true;
        f.Strategy->UpdateCompactions();
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 tacId = f.Backend.OnlyStarted();
        UNIT_ASSERT(f.Backend.Started.at(tacId)->Mode == TCompactionParams::EMode::TierAware);
        // Successful TAC resets the counter even when its data stays in the
        // source tier; physical cooling is not the success criterion.
        f.Complete(tacId, {ColdTier});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 0);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
    }

    Y_UNIT_TEST(ReservedEpochsDoNotTriggerRepeatedCompactionOfAFreeColdSingleton) {
        TStrategyFixture f;
        f.AddPart(20, 1);
        f.AddPart(30, 1, 2);
        const auto free = f.AddPart(10, 4, 3);
        const auto reserved = f.AddPart(25, 4, 4);
        f.AddPart(60, 5, 5);
        f.AddPart(61, 5, 6);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[3].ForceCountToCompact = 2;
        f.Policy->Generations[4].ForceCountToCompact = 2;
        f.Start();
        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(Contains(f.Backend.Started.at(hotId)->Parts, reserved->Label));

        // This task was queued before gen1 reserved one of its two epochs.
        // Only epoch10 remains free, so the last cold generation must decline it.
        UNIT_ASSERT(f.Broker.RunPending(4));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        UNIT_ASSERT(f.Backend.Rejected.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(4), 0);
        UNIT_ASSERT(f.Broker.RunPending(5));
        const ui64 archiveId = f.Backend.StartedGeneration(5);
        f.Complete(archiveId, {});

        // Rechecking generations after unrelated work must not enqueue that
        // same singleton again while the other epoch is still reserved.
        UNIT_ASSERT(!f.Broker.RunPending(4));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        UNIT_ASSERT(f.State.PartLevels.contains(free->Label));
        f.Complete(hotId, {});
        UNIT_ASSERT(f.State.PartLevels.contains(free->Label));
        UNIT_ASSERT(!f.Broker.RunPending(4));
    }

    Y_UNIT_TEST(FiveConflictsDrainRunningWorkAndPrioritizeTheBlockedGeneration) {
        TStrategyFixture f;
        f.AddPart(40, 1);
        f.AddPart(41, 1, 2);
        f.AddPart(10, 3, 3);
        f.AddPart(50, 3, 4);
        f.AddPart(60, 5, 5);
        f.AddPart(61, 5, 6);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Policy->Generations[4].ForceCountToCompact = 2;
        f.Start();

        ui64 hotId = 0;
        for (ui32 attempt = 1; attempt <= 5; ++attempt) {
            UNIT_ASSERT(f.Broker.RunPending(1));
            hotId = f.Backend.StartedGeneration(1);
            UNIT_ASSERT(f.Broker.RunPending(3));
            UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
            UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), attempt);
            UNIT_ASSERT(!f.Broker.RunPending(3));
            if (attempt < 5) {
                UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
                f.Cancel(hotId);
            }
        }
        UNIT_ASSERT(f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT_VALUES_EQUAL(*f.Strategy->GetSerialTierCompactionGeneration(), 3);
        UNIT_ASSERT(f.Backend.Started.contains(hotId));
        UNIT_ASSERT(!f.Broker.RunPending(5));

        f.Complete(hotId, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 5);
        UNIT_ASSERT(f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 cancelledId = f.Backend.OnlyStarted();
        UNIT_ASSERT(f.Backend.Started.at(cancelledId)->Mode == TCompactionParams::EMode::TierAware);
        f.Cancel(cancelledId);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 5);
        UNIT_ASSERT(f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT(!f.Broker.RunPending(5));
        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 retryId = f.Backend.OnlyStarted();
        UNIT_ASSERT(retryId != cancelledId);
        UNIT_ASSERT(f.Backend.Started.at(retryId)->Mode == TCompactionParams::EMode::TierAware);
        f.Complete(retryId, {ColdTier});

        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 0);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT(f.Broker.RunPending(5));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(f.Backend.OnlyStarted())->TierId, ArchiveTier);
    }

    Y_UNIT_TEST(FinalPartsHaveASeparateConflictCounter) {
        TStrategyFixture f;
        f.AddPart(20, 1);
        f.AddPart(30, 1, 2);
        f.AddPart(10, 255, 3);
        f.AddPart(40, 255, 4);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Start();

        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(255));
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Rejected.back(), 255);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(255), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(1), 0);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
    }

    Y_UNIT_TEST(TheConfiguredConflictLimitControlsSerialFallback) {
        TStrategyFixture f;
        f.Policy->TieringCompactionConflictLimit = 1;
        f.AddPart(40, 1);
        f.AddPart(41, 1, 2);
        f.AddPart(10, 3, 3);
        f.AddPart(50, 3, 4);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();

        UNIT_ASSERT(f.Broker.RunPending(1));
        const ui64 hotId = f.Backend.StartedGeneration(1);
        UNIT_ASSERT(f.Broker.RunPending(3));
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 1);
        UNIT_ASSERT(f.Strategy->GetSerialTierCompactionGeneration());
        UNIT_ASSERT_VALUES_EQUAL(*f.Strategy->GetSerialTierCompactionGeneration(), 3);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), hotId);
        f.Complete(hotId, {});

        UNIT_ASSERT(f.Broker.RunPending(3));
        const ui64 coldId = f.Backend.OnlyStarted();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(coldId)->Context->Inputs.size(), 2);
        UNIT_ASSERT(f.Backend.Started.at(coldId)->Mode == TCompactionParams::EMode::TierAware);
        f.Complete(coldId, {ColdTier});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 0);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
    }

    Y_UNIT_TEST(AConflictingForcedMemoryRequestWaitsWithoutFinishingItsUserId) {
        TStrategyFixture f;
        f.AddPart(20, 1);
        f.AddPart(30, 1, 2);
        f.Policy->Generations[0].ForceCountToCompact = 2;
        f.Backend.MemoryEpoch = TEpoch::FromIndex(40);
        f.Start();
        const ui64 diskId = f.RunPending();
        constexpr ui64 forcedId = 777;
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->BeginMemCompaction(0,
            {0, TEpoch::Max()}, forcedId), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.OnlyStarted(), diskId);
        UNIT_ASSERT(f.Backend.Cancelled.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.MemoryRetryRequests, 0);
        f.Strategy->UpdateCompactions();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.MemoryRetryRequests, 0);

        f.Complete(diskId, {});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), 0);
        UNIT_ASSERT(f.Backend.MemoryRetryRequests > 0);
        const ui64 memoryId = f.Strategy->BeginMemCompaction(0,
            {0, TEpoch::Max()}, forcedId);
        UNIT_ASSERT(memoryId != 0);
        UNIT_ASSERT(f.Backend.Started.at(memoryId)->Mode == TCompactionParams::EMode::Regular);
        const ui32 retryRequests = f.Backend.MemoryRetryRequests;
        f.Complete(memoryId, {0});
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), forcedId);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.MemoryRetryRequests, retryRequests);
        f.Strategy->UpdateCompactions();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.MemoryRetryRequests, retryRequests);
    }

    Y_UNIT_TEST(ForcedMemoryCompactionCoversAllTiersAndCompletesTheUserId) {
        TStrategyFixture f;
        f.AddPart(30, 1);
        f.AddPart(20, 3, 2);
        f.AddPart(10, 5, 3);
        f.Start();
        constexpr ui64 forcedId = 777;
        const ui64 id = f.Strategy->BeginMemCompaction(0, {0, TEpoch::Max()}, forcedId);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(id)->Context->Inputs.size(), 3);
        f.Complete(id, {0, ColdTier, ArchiveTier});

        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), forcedId);
        UNIT_ASSERT(f.Strategy->AllowForcedCompaction());
        UNIT_ASSERT(f.Backend.Started.empty());
    }

    Y_UNIT_TEST(IdleTickCanCoolASingletonWithoutImmediatelyRepeatingIt) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        f.AddPart(10, 3);
        f.Start();
        UNIT_ASSERT(!f.Broker.HasPending());
        f.Strategy->UpdateCompactions();
        const auto id = f.RunPending();
        UNIT_ASSERT(f.Backend.Started.at(id)->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(f.Backend.Started.at(id)->Tiering->AllowCooling);
        f.Complete(id, {ColdTier});

        UNIT_ASSERT(f.Backend.Started.empty());
        UNIT_ASSERT(!f.Broker.HasPending());
    }

    Y_UNIT_TEST(ACancelledSingletonFinalSweepIsRetriedWithoutAnotherTick) {
        TStrategyFixture f;
        f.Backend.Enabled = true;
        const auto final = f.AddPart(10, 255);
        f.Start();
        UNIT_ASSERT(!f.Broker.HasPending());
        f.Strategy->UpdateCompactions();
        const ui64 id = f.RunPending();
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.StartedGeneration(255), id);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(id)->Context->Inputs.size(), 1);
        f.Cancel(id);

        UNIT_ASSERT(f.Backend.Started.empty());
        UNIT_ASSERT(f.State.PartLevels.contains(final->Label));
        UNIT_ASSERT(f.Broker.RunPending(255));
        const ui64 retryId = f.Backend.OnlyStarted();
        UNIT_ASSERT(retryId != id);
        UNIT_ASSERT(f.Backend.Started.at(retryId)->Mode == TCompactionParams::EMode::TierAware);
        UNIT_ASSERT(Contains(f.Backend.Started.at(retryId)->Parts, final->Label));
        f.Complete(retryId, {0});

        UNIT_ASSERT(!f.State.PartLevels.contains(final->Label));
        UNIT_ASSERT(f.Backend.Started.empty());
        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(255), 0);
    }

    Y_UNIT_TEST(TheFlagOffDoesNotScheduleAnIdleCoolingSweep) {
        TStrategyFixture f;
        f.AddPart(10, 3);
        f.Start();
        f.Strategy->UpdateCompactions();

        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT(f.Backend.Started.empty());
    }

    Y_UNIT_TEST(CancellationResubmitsNecessaryCompaction) {
        TStrategyFixture f;
        f.AddPart(10, 3);
        f.AddPart(20, 3, 2);
        f.Policy->Generations[2].ForceCountToCompact = 2;
        f.Start();
        const auto id = f.RunPending();
        UNIT_ASSERT(f.Backend.CancelCompaction(id));
        const auto& cancelled = *f.Backend.Cancelled.at(id);
        f.Broker.FinishTask(cancelled.TaskId, EResourceStatus::Cancelled);
        f.Strategy->CompactionCancelled(id, cancelled);

        UNIT_ASSERT(f.Backend.Started.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetTierCompactionConflicts(3), 0);
        UNIT_ASSERT(!f.Strategy->GetSerialTierCompactionGeneration());
        const auto retryId = f.RunPending();
        UNIT_ASSERT(retryId != id);
        UNIT_ASSERT_VALUES_EQUAL(f.Backend.Started.at(retryId)->Context->Inputs.size(), 2);
    }

}

}
