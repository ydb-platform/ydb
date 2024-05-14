#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/data_events/events.h>


namespace NKikimr::NColumnShard {

class TColumnShard;

struct TBasicTxInfo {
    const NKikimrTxColumnShard::ETransactionKind TxKind;
    const ui64 TxId;
public:
    TBasicTxInfo(const NKikimrTxColumnShard::ETransactionKind& txKind, const ui64 txId)
        : TxKind(txKind)
        , TxId(txId) {
    }
};

struct TFullTxInfo: public TBasicTxInfo {
    ui64 MaxStep = Max<ui64>();
    ui64 MinStep = 0;
    ui64 PlanStep = 0;
    TActorId Source;
    ui64 Cookie = 0;
public:
    TFullTxInfo(const NKikimrTxColumnShard::ETransactionKind& txKind, const ui64 txId)
        : TBasicTxInfo(txKind, txId) {
    }
};

class TTxProposeResult {
public:
    class TProposeResult {
        YDB_READONLY(NKikimrTxColumnShard::EResultStatus, Status, NKikimrTxColumnShard::EResultStatus::PREPARED);
        YDB_READONLY_DEF(TString, StatusMessage);
    public:
        TProposeResult() = default;
        TProposeResult(NKikimrTxColumnShard::EResultStatus status, const TString& statusMessage)
            : Status(status)
            , StatusMessage(statusMessage) {
        }

        bool IsFail() const {
            return Status != NKikimrTxColumnShard::EResultStatus::PREPARED && Status != NKikimrTxColumnShard::EResultStatus::SUCCESS;
        }

        TString DebugString() const {
            return TStringBuilder() << "status=" << (ui64)Status << ";message=" << StatusMessage;
        }
    };

private:
    std::optional<TBasicTxInfo> BaseTxInfo;
    std::optional<TFullTxInfo> FullTxInfo;
    TProposeResult ProposeResult;
public:
    TTxProposeResult(const TBasicTxInfo& txInfo, TProposeResult&& result)
        : BaseTxInfo(txInfo)
        , ProposeResult(std::move(result)) {

    }
    TTxProposeResult(const TFullTxInfo& txInfo, TProposeResult&& result)
        : FullTxInfo(txInfo)
        , ProposeResult(std::move(result)) {

    }

    ui64 GetTxId() const noexcept {
        return FullTxInfo ? FullTxInfo->TxId : BaseTxInfo->TxId;
    }

    bool IsError() const {
        return !!BaseTxInfo;
    }

    const TProposeResult& GetProposeResult() const {
        return ProposeResult;
    }

    const TFullTxInfo& GetFullTxInfoVerified() const {
        AFL_VERIFY(!!FullTxInfo);
        return *FullTxInfo;
    }

    const TBasicTxInfo& GetBaseTxInfoVerified() const {
        AFL_VERIFY(!!BaseTxInfo);
        return *BaseTxInfo;
    }
};

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

        TString DebugString() const {
            return TStringBuilder() << "step=" << Step << ";txId=" << TxId << ";";
        }
    };

    using TBasicTxInfo = TBasicTxInfo;
    using TTxInfo = TFullTxInfo;
    using TProposeResult = TTxProposeResult::TProposeResult;

    class ITransactionOperator {
    protected:
        TTxInfo TxInfo;
    public:
        using TPtr = std::shared_ptr<ITransactionOperator>;
        using TFactory = NObjectFactory::TParametrizedObjectFactory<ITransactionOperator, NKikimrTxColumnShard::ETransactionKind, TTxInfo>;

        ITransactionOperator(const TTxInfo& txInfo)
            : TxInfo(txInfo)
        {}

        ui64 GetTxId() const {
            return TxInfo.TxId;
        }

        virtual bool AllowTxDups() const {
            return false;
        }

        virtual ~ITransactionOperator() {}

        virtual bool TxWithDeadline() const {
            return true;
        }

        virtual bool Parse(TColumnShard& owner, const TString& data) = 0;
        virtual TProposeResult ExecuteOnPropose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const = 0;
        virtual bool CompleteOnPropose(TColumnShard& owner, const TActorContext& ctx) const = 0;

        virtual bool ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) = 0;

        virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool CompleteOnAbort(TColumnShard& owner, const TActorContext& ctx) = 0;

        virtual void RegisterSubscriber(const TActorId&) {
            AFL_VERIFY(false)("message", "Not implemented");
        };
        virtual void OnTabletInit(TColumnShard& /*owner*/) {}
    };

private:
    const TDuration MaxCommitTxDelay = TDuration::Seconds(30);
    TColumnShard& Owner;
    THashMap<ui64, TTxInfo> BasicTxInfo;
    std::set<TPlanQueueItem> DeadlineQueue;
    std::set<TPlanQueueItem> PlanQueue;
    std::set<TPlanQueueItem> RunningQueue;

    THashMap<ui64, ITransactionOperator::TPtr> Operators;

private:
    ui64 GetAllowedStep() const;
    bool AbortTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

    TTxInfo RegisterTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc);
    TTxInfo RegisterTxWithDeadline(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc);

public:
    TTxController(TColumnShard& owner);

    ITransactionOperator::TPtr GetTxOperator(const ui64 txId);
    ITransactionOperator::TPtr GetVerifiedTxOperator(const ui64 txId);

    ui64 GetMemoryUsage() const;
    bool HaveOutdatedTxs() const;

    bool Load(NTabletFlatExecutor::TTransactionContext& txc);

    TTxProposeResult ProposeTransaction(const TBasicTxInfo& txInfo, const TString& txBody, const TActorId source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc);
    void CompleteTransaction(const ui64 txId, const TActorContext& ctx);

    bool ExecuteOnCancel(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    bool CompleteOnCancel(const ui64 txId, const TActorContext& ctx);

    std::optional<TTxInfo> StartPlannedTx();
    void FinishPlannedTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void CompleteRunningTx(const TPlanQueueItem& tx);

    std::optional<TPlanQueueItem> GetPlannedTx() const;
    TPlanQueueItem GetFrontTx() const;
    std::optional<TTxInfo> GetTxInfo(const ui64 txId) const;
    NEvents::TDataEvents::TCoordinatorInfo BuildCoordinatorInfo(const TTxInfo& txInfo) const;

    size_t CleanExpiredTxs(NTabletFlatExecutor::TTransactionContext& txc);
    TDuration GetTxCompleteLag(ui64 timecastStep) const;

    enum class EPlanResult {
        Skipped,
        Planned,
        AlreadyPlanned
    };

    EPlanResult PlanTx(const ui64 planStep, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void OnTabletInit();
};

}

