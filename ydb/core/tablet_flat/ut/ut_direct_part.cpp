#include "flat_executor_ut_common.h"
#include "flat_direct_part_writer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {

using namespace NTabletFlatExecutor;

namespace {

enum : ui32 {
    KeyColumnId = 1,
    ValueColumnId = 2,
};

enum EWriteMode {
    ModeCommit = 0,  // attach the built part within a transaction
    ModeAbort = 1,   // finalize, then release the barrier without committing
};

enum EEv {
    EvStartDirectWrite = EventSpaceBegin(TKikimrEvents::ES_USERSPACE) + 700,
    EvDirectWriteDone,
};

struct TEvStartDirectWrite : public TEventLocal<TEvStartDirectWrite, EvStartDirectWrite> {
    TEvStartDirectWrite(ui32 tableId, TVector<std::pair<i64, TString>> rows, EWriteMode mode)
        : TableId(tableId)
        , Rows(std::move(rows))
        , Mode(mode)
    { }

    ui32 TableId;
    TVector<std::pair<i64, TString>> Rows;
    EWriteMode Mode;
    // Optional row to write atomically into another table in the attach tx.
    bool HasCross = false;
    ui32 CrossTableId = 0;
    i64 CrossKey = 0;
    TString CrossValue;
    // Rewrite every blob put result to a non-OK status, forcing the writer to fail.
    bool FailPuts = false;
    // Respect CanFeed() backpressure: feed only while the writer accepts rows and
    // resume on blob put results, instead of feeding the whole batch at once.
    bool Throttle = false;
};

struct TEvDirectWriteDone : public TEventLocal<TEvDirectWriteDone, EvDirectWriteDone> {
    bool Ok;
    TString Error;
    // Set when the writer refused a row via CanFeed() at least once (throttle mode).
    bool SawBackpressure = false;

    explicit TEvDirectWriteDone(bool ok, TString error = {})
        : Ok(ok)
        , Error(std::move(error))
    { }
};

class TTxInitSchema : public ITransaction {
public:
    explicit TTxInitSchema(TVector<ui32> tableIds)
        : TableIds(std::move(tableIds))
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TCompactionPolicy policy;
        policy.MinBTreeIndexNodeSize = 128;

        for (ui32 tableId : TableIds) {
            if (txc.DB.GetScheme().GetTableInfo(tableId)) {
                continue;
            }
            txc.DB.Alter()
                .AddTable("test" + ToString(tableId), tableId)
                .AddColumn(tableId, "key", KeyColumnId, NScheme::TInt64::TypeId, false, false)
                .AddColumn(tableId, "value", ValueColumnId, NScheme::TString::TypeId, false, false)
                .AddColumnToKey(tableId, KeyColumnId)
                .SetCompactionPolicy(tableId, policy);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    TVector<ui32> TableIds;
};

// Reads the whole table at HEAD into a vector of (key, value) pairs.
class TTxReadAll : public ITransaction {
public:
    TTxReadAll(ui32 tableId, TVector<std::pair<i64, TString>>& out)
        : TableId(tableId)
        , Out(out)
    {
        Out.clear();
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Out.clear();
        TVector<NTable::TTag> tags{ KeyColumnId, ValueColumnId };
        auto iter = txc.DB.IterateRange(TableId, { }, tags);
        while (iter->Next(ENext::Data) == EReady::Data) {
            const auto& row = iter->Row();
            i64 key = row.Get(0).AsValue<i64>();
            TString value(row.Get(1).AsBuf());
            Out.emplace_back(key, std::move(value));
        }
        return iter->Last() != EReady::Page;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    TVector<std::pair<i64, TString>>& Out;
};

// Regular writes going through the memtable (upserts or erases), used to verify
// that they layer on top of a directly-written bottom part.
class TTxUpsertRows : public ITransaction {
public:
    TTxUpsertRows(ui32 tableId, TVector<std::pair<i64, TString>> rows, bool erase)
        : TableId(tableId)
        , Rows(std::move(rows))
        , Erase(erase)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        for (const auto& [k, v] : Rows) {
            i64 key = k;
            const auto keyCell = NScheme::TInt64::TInstance(key);
            if (Erase) {
                txc.DB.Update(TableId, NTable::ERowOp::Erase, { keyCell }, { });
            } else {
                TString value = v;
                const auto valCell = NScheme::TString::TInstance(value);
                NTable::TUpdateOp op{ ValueColumnId, NTable::ECellOp::Set, valCell };
                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { keyCell }, { op });
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(ctx.SelfID, new NFake::TEvReturn);
    }

private:
    ui32 TableId;
    TVector<std::pair<i64, TString>> Rows;
    bool Erase;
};

// Attaches a directly-written part as a bottom layer, optionally also writing a
// row to another table in the same transaction (cross-table atomicity).
class TTxAttachPart : public ITransaction {
public:
    TTxAttachPart(TActorId owner, ui32 tableId, THolder<TDirectPartResult> result,
                  bool hasCross, ui32 crossTableId, i64 crossKey, TString crossValue,
                  bool sawBackpressure)
        : Owner(owner)
        , TableId(tableId)
        , Result(std::move(result))
        , HasCross(hasCross)
        , CrossTableId(crossTableId)
        , CrossKey(crossKey)
        , CrossValue(std::move(crossValue))
        , SawBackpressure(sawBackpressure)
    { }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (HasCross) {
            const auto key = NScheme::TInt64::TInstance(CrossKey);
            const auto val = NScheme::TString::TInstance(CrossValue);
            NTable::TUpdateOp op{ ValueColumnId, NTable::ECellOp::Set, val };
            txc.DB.Update(CrossTableId, NTable::ERowOp::Upsert, { key }, { op });
        }
        txc.Env.AttachPart(TableId, std::move(Result));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto* done = new TEvDirectWriteDone(true);
        done->SawBackpressure = SawBackpressure;
        ctx.Send(Owner, done);
    }

private:
    TActorId Owner;
    ui32 TableId;
    THolder<TDirectPartResult> Result;
    bool HasCross;
    ui32 CrossTableId;
    i64 CrossKey;
    TString CrossValue;
    bool SawBackpressure;
};

// A test tablet that owns a TDirectPartWriter, feeds it rows, routes blob put
// results to it, and commits (or aborts) the built part.
class TDirectWriteTablet : public TActor<TDirectWriteTablet>, public TTabletExecutedFlat {
public:
    TDirectWriteTablet(const TActorId& tablet, TTabletStorageInfo* info, const TActorId& owner)
        : TActor(&TDirectWriteTablet::Inbox)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Owner(owner)
    { }

private:
    using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;

    void Inbox(TEventHandlePtr& eh) {
        if (auto* ev = eh->CastAsLocal<NFake::TEvExecute>()) {
            for (auto& tx : ev->Txs) {
                Execute(tx.Release(), ActorContext());
            }
            for (auto& lambda : ev->Lambdas) {
                std::move(lambda)(Executor(), ActorContext());
            }
        } else if (eh->CastAsLocal<NFake::TEvReturn>()) {
            Send(Owner, new TEvents::TEvWakeup);
        } else if (auto* ev = eh->CastAsLocal<TEvStartDirectWrite>()) {
            StartWrite(ev, ActorContext());
        } else if (auto* ev = eh->CastAsLocal<TEvBlobStorage::TEvPutResult>()) {
            if (Writer) {
                if (FailPuts) {
                    // Simulate a storage failure for a direct-write blob.
                    ev->Status = NKikimrProto::ERROR;
                }
                Writer->Handle(*ev, ActorContext());
                if (Feeding && !Writer->HasError()) {
                    FeedSome(ActorContext());
                } else {
                    TryComplete(ActorContext());
                }
            }
        } else if (eh->CastAsLocal<TEvents::TEvPoison>()) {
            if (std::exchange(Stopping, true) != true) {
                auto ctx = ActorContext();
                Executor()->DetachTablet(), Detach(ctx);
            }
        } else if (State == EBoot) {
            TTabletExecutedFlat::StateInitImpl(eh, SelfId());
        } else if (eh->CastAsLocal<TEvTabletPipe::TEvServerConnected>()) {
        } else if (eh->CastAsLocal<TEvTabletPipe::TEvServerDisconnected>()) {
        } else if (!TTabletExecutedFlat::HandleDefaultEvents(eh, SelfId())) {
            Y_TABLET_ERROR("Unexpected event " << eh->GetTypeName());
        }
    }

    void StartWrite(TEvStartDirectWrite* msg, const TActorContext& ctx) {
        WriteTableId = msg->TableId;
        Mode = msg->Mode;
        HasCross = msg->HasCross;
        CrossTableId = msg->CrossTableId;
        CrossKey = msg->CrossKey;
        CrossValue = msg->CrossValue;
        FailPuts = msg->FailPuts;
        Throttle = msg->Throttle;

        Writer = Executor()->BeginWritePart(WriteTableId);
        Y_ENSURE(Writer, "BeginWritePart returned no writer");

        if (Throttle) {
            // Feed lazily, respecting CanFeed() backpressure (see FeedSome).
            PendingRows = msg->Rows;
            Cursor = 0;
            Feeding = true;
            FeedSome(ctx);
            return;
        }

        bool failed = false;
        for (const auto& [k, v] : msg->Rows) {
            if (!FeedRow(k, v, ctx)) {
                failed = true;
                break;
            }
        }

        if (failed) {
            ReportFailure(ctx);
            return;
        }

        Writer->Finalize(ctx);
        TryComplete(ctx);
    }

    // Builds a single row from (key, value) and appends it to the writer.
    bool FeedRow(i64 key, const TString& value, const TActorContext& ctx) {
        TCell keyCell = TCell::Make(key);
        TCell valCell(value.data(), value.size());

        NTable::TRowState row(2);
        row.Touch(NTable::ERowOp::Upsert);
        row.Set(0, NTable::ECellOp::Set, keyCell);
        row.Set(1, NTable::ECellOp::Set, valCell);

        TArrayRef<const TCell> keyRef(&keyCell, 1);
        return Writer->AddRow(keyRef, row, TRowVersion::Min(), ctx);
    }

    // Feeds pending rows while the writer accepts them; stops on backpressure and
    // resumes from the blob-put-result handler once in-flight blobs drain.
    void FeedSome(const TActorContext& ctx) {
        while (Cursor < PendingRows.size() && Writer->CanFeed()) {
            const auto& [k, v] = PendingRows[Cursor];
            if (!FeedRow(k, v, ctx)) {
                ReportFailure(ctx);
                return;
            }
            ++Cursor;
        }

        if (Cursor < PendingRows.size()) {
            // The writer refused more rows: the in-flight blob budget is exhausted.
            Y_ENSURE(!Writer->CanFeed(), "Feeding stopped while the writer still accepts rows");
            SawBackpressure = true;
            return;
        }

        Feeding = false;
        Writer->Finalize(ctx);
        TryComplete(ctx);
    }

    void ReportFailure(const TActorContext& ctx) {
        TString error = Writer->Error();
        Executor()->ReleaseWritePart(Writer->Step());
        Writer.Reset();
        ctx.Send(Owner, new TEvDirectWriteDone(false, error));
    }

    void TryComplete(const TActorContext& ctx) {
        if (!Writer) {
            return;
        }
        if (Writer->HasError()) {
            ReportFailure(ctx);
            return;
        }
        if (!Writer->IsComplete()) {
            return;
        }

        if (Mode == ModeAbort) {
            Executor()->ReleaseWritePart(Writer->Step());
            Writer.Reset();
            ctx.Send(Owner, new TEvDirectWriteDone(true));
            return;
        }

        auto result = Writer->ExtractResult(ctx);
        Writer.Reset();
        Execute(new TTxAttachPart(Owner, WriteTableId, std::move(result),
                                  HasCross, CrossTableId, CrossKey, CrossValue,
                                  SawBackpressure), ctx);
    }

    void OnActivateExecutor(const TActorContext&) override {
        State = EWork;
        SignalTabletActive(SelfId());
        Send(Owner, new NFake::TEvReady(TabletID(), SelfId()));
    }

    void DefaultSignalTabletActive(const TActorContext&) override { }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override {
        OnDetach(ctx);
    }

    void OnDetach(const TActorContext&) override {
        PassAway();
    }

    void Enqueue(TEventHandlePtr& eh) override {
        Y_TABLET_ERROR("Got unexpected event " << eh->GetTypeName() << " on tablet booting");
    }

private:
    enum EState { EBoot, EWork };

    TActorId Owner;
    EState State = EBoot;
    bool Stopping = false;

    THolder<TDirectPartWriter> Writer;
    ui32 WriteTableId = 0;
    EWriteMode Mode = ModeCommit;
    bool HasCross = false;
    ui32 CrossTableId = 0;
    i64 CrossKey = 0;
    TString CrossValue;
    bool FailPuts = false;

    // Throttled (backpressure-respecting) feeding state.
    bool Throttle = false;
    bool Feeding = false;
    bool SawBackpressure = false;
    size_t Cursor = 0;
    TVector<std::pair<i64, TString>> PendingRows;
};

struct TDirectWriteEnv : public TMyEnvBase {
    void FireWriteTablet() {
        FireTablet(Edge, Tablet, [this](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TDirectWriteTablet(tablet, info, Edge);
        });
        WaitFor<NFake::TEvReady>();
    }

    void RestartWriteTablet() {
        SendSync(new TEvents::TEvPoison, false, true);
        FireWriteTablet();
    }

    TVector<std::pair<i64, TString>> ReadAll(ui32 tableId) {
        TVector<std::pair<i64, TString>> rows;
        SendSync(new NFake::TEvExecute{ new TTxReadAll(tableId, rows) });
        return rows;
    }

    void Upsert(ui32 tableId, TVector<std::pair<i64, TString>> rows) {
        SendSync(new NFake::TEvExecute{ new TTxUpsertRows(tableId, std::move(rows), /* erase */ false) });
    }

    void Erase(ui32 tableId, const TVector<i64>& keys) {
        TVector<std::pair<i64, TString>> rows;
        for (i64 k : keys) {
            rows.emplace_back(k, TString());
        }
        SendSync(new NFake::TEvExecute{ new TTxUpsertRows(tableId, std::move(rows), /* erase */ true) });
    }
};

TVector<std::pair<i64, TString>> MakeRows(i64 count) {
    TVector<std::pair<i64, TString>> rows;
    for (i64 i = 0; i < count; ++i) {
        rows.emplace_back(i, TStringBuilder() << "value_" << i);
    }
    return rows;
}

// Rows whose values are large enough that feeding many of them exceeds the
// writer's in-flight blob budget (TDirectPartWriter::MaxFlight).
TVector<std::pair<i64, TString>> MakeBigRows(i64 count, size_t valueSize) {
    TVector<std::pair<i64, TString>> rows;
    for (i64 i = 0; i < count; ++i) {
        rows.emplace_back(i, TString(valueSize, char('a' + (i % 26))));
    }
    return rows;
}

} // namespace

Y_UNIT_TEST_SUITE(TDirectPartWrite) {

    Y_UNIT_TEST(HappyPath) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(500);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        env.SendAsync(start);
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(UnsortedInputFails) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        TVector<std::pair<i64, TString>> rows{ {5, "a"}, {3, "b"} };
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT(!done->Get()->Ok);
        UNIT_ASSERT(!done->Get()->Error.empty());

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 0u);
    }

    Y_UNIT_TEST(AbortDoesNotCommit) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(300);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeAbort));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 0u);
    }

    Y_UNIT_TEST(SurvivesRestart) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        auto rows = MakeRows(400);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        env.RestartWriteTablet();

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(CrossTableAtomicity) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101, 102 }) });

        auto rows = MakeRows(200);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->HasCross = true;
        start->CrossTableId = 102;
        start->CrossKey = 777;
        start->CrossValue = "marker";
        env.SendAsync(start);
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        env.RestartWriteTablet();

        auto target = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(target.size(), rows.size());

        auto cross = env.ReadAll(102);
        UNIT_ASSERT_VALUES_EQUAL(cross.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(cross[0].first, 777);
        UNIT_ASSERT_VALUES_EQUAL(cross[0].second, "marker");
    }

    Y_UNIT_TEST(BlobPutFailureFailsWrite) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Enough rows to produce at least one data blob whose put we fail.
        auto rows = MakeRows(500);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->FailPuts = true;
        env.SendAsync(start);

        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT(!done->Get()->Ok);
        UNIT_ASSERT(!done->Get()->Error.empty());

        // Nothing was attached and the tablet stays healthy across a restart.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);
        env.RestartWriteTablet();
        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);
    }

    Y_UNIT_TEST(EmptyInputCommits) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Finalize with zero rows: the commit advances the epoch but attaches no part.
        env.SendAsync(new TEvStartDirectWrite(101, { }, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadAll(101).size(), 0u);

        // The table is still usable afterwards.
        env.Upsert(101, { {1, "one"}, {2, "two"} });
        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(read[0].second, "one");
        UNIT_ASSERT_VALUES_EQUAL(read[1].second, "two");
    }

    Y_UNIT_TEST(BackpressureThrottlesFeeding) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // 200 x 128 KiB = 25 MiB > MaxFlight (20 MiB): feeding must stall at least once.
        const size_t valueSize = 128 * 1024;
        auto rows = MakeBigRows(200, valueSize);
        auto* start = new TEvStartDirectWrite(101, rows, ModeCommit);
        start->Throttle = true;
        env.SendAsync(start);

        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);
        UNIT_ASSERT_C(done->Get()->SawBackpressure,
            "expected CanFeed() to refuse rows at least once");

        auto read = env.ReadAll(101);
        UNIT_ASSERT_VALUES_EQUAL(read.size(), rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read[i].first, rows[i].first);
            UNIT_ASSERT_VALUES_EQUAL(read[i].second, rows[i].second);
        }
    }

    Y_UNIT_TEST(RegularWritesLayerOnTop) {
        TDirectWriteEnv env;
        env.FireWriteTablet();

        env.SendSync(new NFake::TEvExecute{ new TTxInitSchema({ 101 }) });

        // Bottom layer: direct-write keys [0, 100) as "value_<k>".
        auto rows = MakeRows(100);
        env.SendAsync(new TEvStartDirectWrite(101, rows, ModeCommit));
        auto done = env.GrabEdgeEvent<TEvDirectWriteDone>();
        UNIT_ASSERT_C(done->Get()->Ok, done->Get()->Error);

        // Regular writes on top: overwrite even keys, add new keys [100, 110).
        TVector<std::pair<i64, TString>> top;
        for (i64 i = 0; i < 100; i += 2) {
            top.emplace_back(i, TStringBuilder() << "top_" << i);
        }
        for (i64 i = 100; i < 110; ++i) {
            top.emplace_back(i, TStringBuilder() << "top_" << i);
        }
        env.Upsert(101, top);
        // ... and erase one key that only exists in the bottom part.
        env.Erase(101, { 1 });

        // Expected view after layering the memtable over the bottom part.
        TMap<i64, TString> expected;
        for (i64 i = 0; i < 100; ++i) {
            expected[i] = TStringBuilder() << "value_" << i;
        }
        for (const auto& [k, v] : top) {
            expected[k] = v;
        }
        expected.erase(1);

        auto verify = [&](const TString& stage) {
            auto read = env.ReadAll(101);
            UNIT_ASSERT_VALUES_EQUAL_C(read.size(), expected.size(), stage);
            for (const auto& [k, v] : read) {
                auto it = expected.find(k);
                UNIT_ASSERT_C(it != expected.end(), stage << ": unexpected key " << k);
                UNIT_ASSERT_VALUES_EQUAL_C(v, it->second, stage << ": key " << k);
            }
        };

        verify("before restart");
        env.RestartWriteTablet();
        verify("after restart");
    }
}

} // namespace NKikimr::NTable
