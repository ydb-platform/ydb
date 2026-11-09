#pragma once

#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/tablet_flat/flat_comp_gen.h>
#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_comp_create.h>
#include <ydb/core/tablet_flat/flat_exec_broker.h>
#include <ydb/core/tablet_flat/logic_snap_main.h>

#include <util/stream/str.h>

#include <functional>

namespace NKikimr::NTabletFlatExecutor {

// The executor already grants this class access to its compaction internals.
// Only staging/inspection is special: scans, writes, switches and recovery use
// the production executor, BlobStorage mock and compaction strategy.
class TExecutorCompactionLogic {
public:
    static TCompactionLogicState* Seed(TExecutor& executor, ui32 table,
            const std::optional<THashMap<TLogoBlobID, ui32>>& levels = {}) {
        auto snapshot = executor.CompactionLogic->SnapToLog(table);
        if (levels) {
            snapshot.State.PartLevels = *levels;
        }
        executor.CompactionLogic->Stop();
        TAutoPtr<TCompactionLogicState> state = new TCompactionLogicState;
        auto* result = state.Get();
        state->Snapshots[table] = std::move(snapshot);
        executor.CompactionLogic.Reset(new TCompactionLogic(
            executor.MemTableMemoryConsumersCollection.Get(), executor.Logger.Get(),
            executor.Broker.Get(), &executor, std::move(state), "tiering-executor-ut"));
        executor.CompactionLogic->Start();
        return result;
    }

    static std::optional<TTxStamp> Snapshot(TExecutor& executor) {
        if (!executor.LogicSnap->MayFlush(true) || executor.PendingPartSwitches) {
            return std::nullopt;
        }
        executor.MakeLogSnapshot();
        // MakeLogSnapshot finishes one synchronous commit and advances Step.
        return TTxStamp(executor.Generation(), executor.Step() - 1);
    }
    static void Tick(TExecutor& executor) { executor.CompactionLogic->UpdateCompactions(); }
    static void Cancel(TExecutor& executor, ui64 serial) { executor.CancelCompaction(serial); }
    static TVector<NTable::TPartView> Parts(TExecutor& executor, ui32 table) {
        return executor.TableParts(table);
    }
    static THashMap<TLogoBlobID, ui32> Levels(TExecutor& executor, ui32 table) {
        return executor.CompactionLogic->SnapToLog(table).State.PartLevels;
    }
};

namespace NTieringTest {

inline constexpr ui32 TableId = 101;
inline constexpr ui32 KeyTag = 1;
inline constexpr ui32 TimeTag = 2;
inline constexpr ui32 ValueTag = 3;
inline constexpr ui32 ExtraTag = 4;
inline constexpr ui32 ColdTier = 42;
inline constexpr ui32 ArchiveTier = 7;

struct TRow {
    ui64 Key;
    std::optional<ui64> Time;
    TString Value;
    ui64 Extra = 0;
    TRowVersion Version = TRowVersion::Min();
    bool Erase = false;
};

inline NLocalDb::TCompactionPolicyPtr MakePolicy() {
    auto policy = NLocalDb::CreateDefaultUserTablePolicy();
    policy->InMemSizeToSnapshot = ui64(1) << 50;
    policy->InMemForceSizeToSnapshot = ui64(1) << 51;
    policy->InMemStepsToSnapshot = Max<ui32>();
    policy->InMemForceStepsToSnapshot = Max<ui32>();
    policy->LogOverheadSizeToSnapshot = 0;
    policy->LogOverheadCountToSnapshot = 0;
    policy->DroppedRowsPercentToCompact = 101;
    const auto generation = policy->Generations.front();
    policy->Generations.assign(7, generation);
    for (ui32 index = 0; index < policy->Generations.size(); ++index) {
        auto& gen = policy->Generations[index];
        gen.SizeToCompact = ui64(1) << 50;
        gen.ForceSizeToCompact = ui64(1) << 51;
        gen.CountToCompact = 1000;
        gen.ForceCountToCompact = 1000;
        gen.ExtraCompactionPercent = 0;
        gen.ExtraCompactionMinSize = 0;
        gen.ExtraCompactionExpPercent = 0;
        gen.ExtraCompactionExpMaxSize = 0;
        gen.UpliftPartSize = 0;
        gen.BackgroundCompactionPolicy.Threshold = 101;
        gen.TierId = index < 3 ? 0 : index < 5 ? ColdTier : ArchiveTier;
    }
    return policy;
}

inline NTable::TTieringPolicy MakeTierPolicy() {
    return {
        .ColumnTag = TimeTag,
        .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
        .Rules = {
            {.TierId = ArchiveTier, .Ranges = {{
                .To = NTable::TTierRelativeBound{TDuration::Days(365)}, .ToInclusive = true}}},
            {.TierId = ColdTier, .Ranges = {{
                .To = NTable::TTierRelativeBound{TDuration::Days(7)}, .ToInclusive = true}}},
        },
    };
}

class TTx final : public ITransaction {
public:
    explicit TTx(std::function<bool(TTransactionContext&)> execute)
        : ExecuteFn(std::move(execute))
    { }
    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        return ExecuteFn(txc);
    }
    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }
private:
    std::function<bool(TTransactionContext&)> ExecuteFn;
};

class TEnv : public TMyEnvBase {
public:
    TEnv() {
        // TTestActorRuntime installs a mock TimeProvider initially at epoch 0.
        // Relative policies need a positive, deterministic clock before boot.
        Env.UpdateCurrentTime(TInstant::Seconds(2'000'000'000));
        auto& flags = Env.GetAppData().FeatureFlags;
        flags.SetEnableDataTiering(false);
        flags.SetEnableLocalDBBtreeIndex(true);
        flags.SetEnableLocalDBFlatIndex(false);
        FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        Exec([](TTransactionContext& txc) {
            auto policy = MakePolicy();
            txc.DB.Alter()
                .AddTable("tiering-integration", TableId)
                .AddColumn(TableId, "key", KeyTag, NScheme::NTypeIds::Uint64, false, false)
                .AddColumn(TableId, "time", TimeTag, NScheme::NTypeIds::Uint64, false, false)
                .AddColumn(TableId, "value", ValueTag, NScheme::NTypeIds::String, false, false)
                .AddColumn(TableId, "extra", ExtraTag, NScheme::NTypeIds::Uint64, false, false)
                .AddColumnToKey(TableId, KeyTag)
                .SetRoom(TableId, ColdTier, 2, {2}, 2)
                .SetRoom(TableId, ArchiveTier, 3, {3}, 3)
                .AddFamily(TableId, ColdTier, ColdTier)
                .AddFamily(TableId, ArchiveTier, ArchiveTier)
                .SetFamilyBlobs(TableId, ColdTier, 64, 128)
                .SetFamilyBlobs(TableId, ArchiveTier, 64, 128)
                .SetCompactionPolicy(TableId, *policy)
                .SetTieringPolicy(TableId, MakeTierPolicy());
            return true;
        });
    }

    ~TEnv() {
        SendSync(new TEvents::TEvPoison, false, true);
    }

    void Exec(std::function<bool(TTransactionContext&)> fn, bool follower = false) {
        auto* event = new NFake::TEvExecute(new TTx(std::move(fn)));
        if (follower) {
            SendFollowerSync(event);
        } else {
            SendSync(event);
        }
    }

    void RawCall(std::function<void(TExecutor&)> fn) {
        SendSync(new NFake::TEvCall([this, fn = std::move(fn)](
                NFlatExecutorSetup::IExecutor* executor, const TActorContext& ctx) {
            fn(*static_cast<TExecutor*>(executor));
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }));
    }

    void Call(std::function<void(TExecutor&, TCompactionLogicState&)> fn) {
        RawCall([&](TExecutor& executor) {
            if (!State) {
                State = TExecutorCompactionLogic::Seed(executor, TableId);
            }
            fn(executor, *State);
        });
    }

    void Write(TVector<TRow> rows) {
        Exec([rows = std::move(rows)](TTransactionContext& txc) {
            for (const auto& row : rows) {
                const auto key = NScheme::TUint64::TInstance(row.Key);
                if (row.Erase) {
                    txc.DB.Update(TableId, NTable::ERowOp::Erase, {key}, {}, row.Version);
                    continue;
                }
                const auto value = NScheme::TString::TInstance(row.Value);
                const auto extra = NScheme::TUint64::TInstance(row.Extra);
                const auto time = NScheme::TUint64::TInstance(row.Time.value_or(0));
                TVector<NTable::TUpdateOp> ops;
                if (row.Time) {
                    ops.push_back({TimeTag, NTable::ECellOp::Set, time});
                } else {
                    ops.push_back({TimeTag, NTable::ECellOp::Set,
                        TRawTypeValue(nullptr, 0, NScheme::NTypeIds::Uint64)});
                }
                ops.push_back({ValueTag, NTable::ECellOp::Set, value});
                ops.push_back({ExtraTag, NTable::ECellOp::Set, extra});
                txc.DB.Update(TableId, NTable::ERowOp::Upsert, {key}, ops, row.Version);
            }
            return true;
        });
    }

    void Flush() {
        SendSync(new NFake::TEvCompact(TableId, true));
        WaitFor<NFake::TEvCompacted>();
    }

    TVector<NTable::TPartView> Parts() {
        TVector<NTable::TPartView> parts;
        RawCall([&](TExecutor& executor) { parts = TExecutorCompactionLogic::Parts(executor, TableId); });
        return parts;
    }

    THashMap<TLogoBlobID, ui32> Levels() {
        THashMap<TLogoBlobID, ui32> levels;
        RawCall([&](TExecutor& executor) { levels = TExecutorCompactionLogic::Levels(executor, TableId); });
        return levels;
    }

    void SeedLevels(const THashMap<TLogoBlobID, ui32>& levels,
            const NLocalDb::TCompactionPolicy* policy = nullptr) {
        RawCall([&](TExecutor& executor) {
            State = TExecutorCompactionLogic::Seed(executor, TableId, levels);
        });
        if (policy) {
            Policy(*policy);
        }
    }

    void Policy(const NLocalDb::TCompactionPolicy& policy) {
        TierIds.clear();
        for (const auto& generation : policy.Generations) {
            TierIds.push_back(generation.TierId.value_or(0));
        }
        Exec([&](TTransactionContext& txc) {
            txc.DB.Alter().SetCompactionPolicy(TableId, policy);
            return true;
        });
    }

    void Enable(bool value = true) { Env.GetAppData().FeatureFlags.SetEnableDataTiering(value); }

    void Tick() { RawCall([](TExecutor& executor) { TExecutorCompactionLogic::Tick(executor); }); }

    void Restart() {
        THashSet<ui64> confirmed;
        auto observer = Env.AddObserver<TEvTablet::TEvCommitResult>([&](auto& event) {
            const auto* result = event->Get();
            if (result->TabletID == Tablet && event->Cookie == ui64(ECommit::Snap)) {
                UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
                confirmed.insert(TTxStamp(result->Generation, result->Step));
            }
        });
        std::optional<TTxStamp> snapshot;
        for (ui32 attempt = 0; !snapshot && attempt < 100; ++attempt) {
            // A previous snapshot can still be in flight. Actor calls let its
            // confirmation progress before requesting the snapshot we need.
            RawCall([&](TExecutor& executor) { snapshot = TExecutorCompactionLogic::Snapshot(executor); });
        }
        UNIT_ASSERT_C(snapshot, "Could not start a snapshot before restarting the tiered tablet");
        Env.WaitFor("durable tiering snapshot", [&] { return confirmed.contains(ui64(*snapshot)); });
        observer.Remove();
        SendSync(new TEvents::TEvPoison, false, true);
        State = nullptr;
        // Activation calls UpdateCompactions synchronously before TEvReady.
        // Inspect the recovered placement before that initial policy sweep
        // can replace parts; tests trigger subsequent TAC explicitly.
        const bool enableTiering = Env.GetAppData().FeatureFlags.GetEnableDataTiering();
        Enable(false);
        FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
        Enable(enableTiering);
    }

    ui32 Tier(ui32 level) const {
        return level == 255 ? 0 : TierIds.at(level - 1);
    }

    void CheckLevels(const THashMap<TLogoBlobID, ui32>& expected, TStringBuf stage = {}) {
        const auto actual = Levels();
        TStringStream diagnostic;
        diagnostic << stage << " expected levels:";
        for (const auto& [label, level] : expected) {
            diagnostic << ' ' << label << '=' << level;
        }
        diagnostic << "; actual levels:";
        for (const auto& [label, level] : actual) {
            diagnostic << ' ' << label << '=' << level;
        }
        UNIT_ASSERT_C(actual == expected, diagnostic.Str());
    }

    ui64 NowSeconds() const { return Env.GetCurrentTime().Seconds(); }

    NTable::TPartView FlushPart(TVector<TRow> rows) {
        THashSet<TLogoBlobID> before;
        for (const auto& part : Parts()) {
            before.insert(part->Label);
        }
        Write(std::move(rows));
        Flush();
        TVector<NTable::TPartView> added;
        for (const auto& part : Parts()) {
            if (!before.contains(part->Label)) {
                added.push_back(part);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(added.size(), 1);
        return added.front();
    }

    void Read(ui64 key, const TString& expected,
            TRowVersion version = TRowVersion::Max()) {
        Exec([=](TTransactionContext& txc) {
            const auto rawKey = NScheme::TUint64::TInstance(key);
            const TVector<NTable::TTag> tags{ValueTag};
            NTable::TRowState row;
            const auto ready = txc.DB.Select(TableId, {rawKey}, tags, row, 0, version);
            if (ready == NTable::EReady::Page) {
                return false;
            }
            UNIT_ASSERT(ready == NTable::EReady::Data);
            const auto cell = row.Get(0);
            UNIT_ASSERT_VALUES_EQUAL(TString(cell.Data(), cell.Size()), expected);
            return true;
        });
    }

    TCompactionLogicState* State = nullptr;
    TVector<ui32> TierIds = {0, 0, 0, ColdTier, ColdTier, ArchiveTier, ArchiveTier};
};

}
}
