#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/data_events/events.h>


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

    class TProposeResult {
        YDB_READONLY(NKikimrTxColumnShard::EResultStatus, Status, NKikimrTxColumnShard::EResultStatus::PREPARED);
        YDB_READONLY_DEF(TString, StatusMessage);
    public:
        TProposeResult() = default;
        TProposeResult(NKikimrTxColumnShard::EResultStatus status, const TString& statusMessage)
            : Status(status)
            , StatusMessage(statusMessage)
        {}

        bool operator!() const {
            return Status != NKikimrTxColumnShard::EResultStatus::PREPARED;
        }
    };

    class ITransactionOperatior {
    protected:
        TBasicTxInfo TxInfo;
    public:
        using TPtr = std::shared_ptr<ITransactionOperatior>;
        using TFactory = NObjectFactory::TParametrizedObjectFactory<ITransactionOperatior, NKikimrTxColumnShard::ETransactionKind, TBasicTxInfo>;

        ITransactionOperatior(const TBasicTxInfo& txInfo)
            : TxInfo(txInfo)
        {}

        ui64 GetTxId() const {
            return TxInfo.TxId;
        }

        virtual ~ITransactionOperatior() {}

        virtual bool TxWithDeadline() const {
            return true;
        }

        virtual bool Parse(const TString& data) = 0;
        virtual TProposeResult Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, bool proposed) const = 0;

        virtual bool Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool Complete(TColumnShard& owner, const TActorContext& ctx) = 0;
        virtual void RegisterSubscriber(const TActorId&) {
            AFL_VERIFY(false)("message", "Not implemented");
        };
        virtual void OnTabletInit(TColumnShard& /*owner*/) {}
    };

private:
    const TDuration MaxCommitTxDelay = TDuration::Seconds(30);
    TColumnShard& Owner;
    THashMap<ui64, TBasicTxInfo> BasicTxInfo;
    std::set<TPlanQueueItem> DeadlineQueue;
    std::set<TPlanQueueItem> PlanQueue;
    std::set<TPlanQueueItem> RunningQueue;

    THashMap<ui64, ITransactionOperatior::TPtr> Operators;

private:
    ui64 GetAllowedStep() const;
    bool AbortTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

public:
    TTxController(TColumnShard& owner);

    ITransactionOperatior::TPtr GetTxOperator(const ui64 txId);
    ITransactionOperatior::TPtr GetVerifiedTxOperator(const ui64 txId);

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
    void OnTabletInit();
};

}

