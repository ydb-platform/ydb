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
};

struct TEvDirectWriteDone : public TEventLocal<TEvDirectWriteDone, EvDirectWriteDone> {
    bool Ok;
    TString Error;

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

// Attaches a directly-written part as a bottom layer, optionally also writing a
// row to another table in the same transaction (cross-table atomicity).
class TTxAttachPart : public ITransaction {
public:
    TTxAttachPart(TActorId owner, ui32 tableId, THolder<TDirectPartResult> result,
                  bool hasCross, ui32 crossTableId, i64 crossKey, TString crossValue)
        : Owner(owner)
        , TableId(tableId)
        , Result(std::move(result))
        , HasCross(hasCross)
        , CrossTableId(crossTableId)
        , CrossKey(crossKey)
        , CrossValue(std::move(crossValue))
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
        ctx.Send(Owner, new TEvDirectWriteDone(true));
    }

private:
    TActorId Owner;
    ui32 TableId;
    THolder<TDirectPartResult> Result;
    bool HasCross;
    ui32 CrossTableId;
    i64 CrossKey;
    TString CrossValue;
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
                Writer->Handle(*ev, ActorContext());
                TryComplete(ActorContext());
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

        Writer = Executor()->BeginWritePart(WriteTableId);
        Y_ENSURE(Writer, "BeginWritePart returned no writer");

        bool failed = false;
        for (const auto& [k, v] : msg->Rows) {
            i64 key = k;
            TString value = v;
            TCell keyCell = TCell::Make(key);
            TCell valCell(value.data(), value.size());

            NTable::TRowState row(2);
            row.Touch(NTable::ERowOp::Upsert);
            row.Set(0, NTable::ECellOp::Set, keyCell);
            row.Set(1, NTable::ECellOp::Set, valCell);

            TArrayRef<const TCell> keyRef(&keyCell, 1);
            if (!Writer->AddRow(keyRef, row, TRowVersion::Min(), ctx)) {
                failed = true;
                break;
            }
        }

        if (failed) {
            TString error = Writer->Error();
            Executor()->ReleaseWritePart(Writer->Step());
            Writer.Reset();
            ctx.Send(Owner, new TEvDirectWriteDone(false, error));
            return;
        }

        Writer->Finalize(ctx);
        TryComplete(ctx);
    }

    void TryComplete(const TActorContext& ctx) {
        if (!Writer || !Writer->IsComplete()) {
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
                                  HasCross, CrossTableId, CrossKey, CrossValue), ctx);
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
};

TVector<std::pair<i64, TString>> MakeRows(i64 count) {
    TVector<std::pair<i64, TString>> rows;
    for (i64 i = 0; i < count; ++i) {
        rows.emplace_back(i, TStringBuilder() << "value_" << i);
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
}

} // namespace NKikimr::NTable
