#pragma once

#include "columnshard_schema.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/ev_write/events.h>


namespace NKikimr::NColumnShard {

class TColumnShard;

class TTxController {
public:
    struct TPlanQueueItem {
        const ui64 Step = 0;
        const ui64 TxId = 0;

        TPlanQueueItem(const ui64 step, const ui64 txId)
            : Step(step)
            , TxId(txId)
        {}

        inline bool operator<(const TPlanQueueItem& rhs) const {
            return Step < rhs.Step || (Step == rhs.Step && TxId < rhs.TxId);
        }
    };

    struct TBasicTxInfo {
        ui64 TxId;
        ui64 MaxStep = Max<ui64>();
        ui64 MinStep = 0;
        ui64 PlanStep = 0;
        TActorId Source;
        ui64 Cookie = 0;
        NKikimrTxColumnShard::ETransactionKind TxKind;
    };

private:
    const TDuration MaxCommitTxDelay = TDuration::Seconds(30);
    TColumnShard& Owner;
    THashMap<ui64, TBasicTxInfo> BasicTxInfo;
    std::set<TPlanQueueItem> DeadlineQueue;
    std::set<TPlanQueueItem> PlanQueue;
    std::set<TPlanQueueItem> RunningQueue;

private:
    ui64 GetAllowedStep() const;
    bool AbortTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

public:
    TTxController(TColumnShard& owner);

    void Clear();

    ui64 GetMemoryUsage() const;
    bool HaveOutdatedTxs() const;

    bool Load(NTabletFlatExecutor::TTransactionContext& txc);

    const TBasicTxInfo& RegisterTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc);
    const TBasicTxInfo& RegisterTxWithDeadline(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc);

    bool CancelTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

    std::optional<TBasicTxInfo> StartPlannedTx();
    void FinishPlannedTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void CompleteRunningTx(const TPlanQueueItem& tx);

    std::optional<TPlanQueueItem> GetPlannedTx() const;
    TPlanQueueItem GetFrontTx() const;
    const TBasicTxInfo* GetTxInfo(const ui64 txId) const;
    NEvents::TDataEvents::TCoordinatorInfo GetCoordinatorInfo(const ui64 txId) const;

    size_t CleanExpiredTxs(NTabletFlatExecutor::TTransactionContext& txc);

    enum class EPlanResult {
        Skipped,
        Planned,
        AlreadyPlanned
    };

    EPlanResult PlanTx(const ui64 planStep, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
};

}
