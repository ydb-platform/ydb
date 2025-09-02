#pragma once
#include "defs.h"
#include "flat_sausage_grind.h"
#include "flat_sausage_slicer.h"
#include "flat_dbase_change.h"
#include "flat_exec_seat.h"
#include "flat_exec_commit.h"
#include "flat_executor_counters.h"
#include "flat_sausage_slicer.h"
#include "logic_redo_eggs.h"
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

class TCommitManager;

namespace NRedo {
    struct TBatch;
    struct TEntry;
    struct TQueue;
}

class TLogicRedo {
    using TMonCo = TExecutorCounters;

    TCommitManager * const CommitManager;
    TAutoPtr<NPageCollection::TSteppedCookieAllocator> Cookies;
    TAutoPtr<NRedo::TBatch> Batch;
    TAutoPtr<NRedo::TQueue> Queue;
    NPageCollection::TSlicer Slicer;

    TExecutorCounters *Counters = nullptr;
    TTabletCountersWithTxTypes *AppTxCounters = nullptr;

    struct TCompletionEntry {
        ui32 Step;

        // The first owned tx is rw
        TIntrusiveListWithAutoDelete<TSeat, TDelete> Transactions;

        TCompletionEntry(std::unique_ptr<TSeat> seat, ui32 step);
    };

    TDeque<TCompletionEntry> CompletionQueue; // would be graph once data-dependencies implemented
    ui32 PrevConfirmedStep = 0;

public:
    struct TCommitRWTransactionResult {
        TAutoPtr<TLogCommit> Commit;
        bool NeedFlush;
    };

    TLogicRedo(TAutoPtr<NPageCollection::TSteppedCookieAllocator>, TCommitManager*, TAutoPtr<NRedo::TQueue>);
    ~TLogicRedo();

    void Describe(IOutputStream &out) const;
    void InstallCounters(TExecutorCounters *counters, TTabletCountersWithTxTypes* appTxCounters);
    bool CommitROTransaction(std::unique_ptr<TSeat> seat, const TActorContext &ownerCtx);
    TCommitRWTransactionResult CommitRWTransaction(std::unique_ptr<TSeat> seat, NTable::TChange &change, bool force);
    void MakeLogEntry(TLogCommit&, TString redo, TArrayRef<const ui32> affects, bool embed);
    void FlushBatchedLog();

    ui64 Confirm(ui32 step, const TActorContext &ctx, const TActorId &ownerId);

    void CutLog(ui32 table, NTable::TSnapEdge, TGCBlobDelta&);
    void SnapToLog(NKikimrExecutorFlat::TLogSnapshot&);
    NRedo::TStats LogStats() const;
    TArrayRef<const NRedo::TUsage> GrabLogUsage() const;
};

void CompleteRoTransaction(std::unique_ptr<TSeat>, const TActorContext &ownerCtx, TExecutorCounters *counters, TTabletCountersWithTxTypes *appTxCounters);

}}
