#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/message_seqno.h>


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

    bool operator==(const TBasicTxInfo& item) const = default;

    ui64 GetTxId() const {
        return TxId;
    }

    TString DebugString() const {
        return TStringBuilder() << TxId << ":" << NKikimrTxColumnShard::ETransactionKind_Name(TxKind);
    }
};

struct TFullTxInfo: public TBasicTxInfo {
private:
    using TBase = TBasicTxInfo;

public:
    ui64 MaxStep = Max<ui64>();
    ui64 MinStep = 0;
    ui64 PlanStep = 0;
    TActorId Source;
    ui64 Cookie = 0;
    std::optional<TMessageSeqNo> SeqNo;
public:
    bool operator==(const TFullTxInfo& item) const = default;

    TString DebugString() const {
        TStringBuilder sb;
        sb << TBase::DebugString() << ";min=" << MinStep << ";max=" << MaxStep << ";plan=" << PlanStep << ";src=" << Source << ";cookie=" << Cookie;
        if (SeqNo) {
            sb << *SeqNo << ";";
        }
        return sb;
    }

    TString SerializeSeqNoAsString() const {
        if (!SeqNo) {
            return "";
        }
        return SeqNo->SerializeToString();
    }

    void DeserializeSeqNoFromString(const TString& data) {
        if (!data) {
            SeqNo = std::nullopt;
        } else {
            TMessageSeqNo seqNo;
            seqNo.DeserializeFromString(data).Validate();
            SeqNo = seqNo;
        }
    }

    TFullTxInfo(const NKikimrTxColumnShard::ETransactionKind& txKind, const ui64 txId)
        : TBasicTxInfo(txKind, txId) {
    }

    TFullTxInfo(const NKikimrTxColumnShard::ETransactionKind& txKind, const ui64 txId, const TActorId& source, const ui64 cookie, const std::optional<TMessageSeqNo>& seqNo)
        : TBasicTxInfo(txKind, txId)
        , Source(source)
        , Cookie(cookie)
        , SeqNo(seqNo)
    {
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
    public:
        enum class EStatus {
            Created,
            Parsed,
            ProposeStartedOnExecute,
            ProposeStartedOnComplete,
            ProposeFinishedOnExecute,
            ProposeFinishedOnComplete,
            ReplySent,
            Failed
        };
    protected:
        TTxInfo TxInfo;
        YDB_READONLY_DEF(std::optional<TTxController::TProposeResult>, ProposeStartInfo);
        std::optional<EStatus> Status = EStatus::Created;
    private:
        friend class TTxController;
        virtual bool DoParse(TColumnShard& owner, const TString& data) = 0;
        virtual TTxController::TProposeResult DoStartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual void DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& ctx) = 0;
        virtual void DoFinishProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual void DoFinishProposeOnComplete(TColumnShard& owner, const TActorContext& ctx) = 0;
        virtual bool DoIsAsync() const = 0;
        virtual void DoSendReply(TColumnShard& owner, const TActorContext& ctx) = 0;
        virtual bool DoCheckAllowUpdate(const TFullTxInfo& currentTxInfo) const = 0;
        virtual bool DoCheckTxInfoForReply(const TFullTxInfo& /*originalTxInfo*/) const {
            return true;
        }

        void SwitchStateVerified(const EStatus from, const EStatus to);
        TTxInfo& MutableTxInfo() {
            return TxInfo;
        }

        virtual void DoOnTabletInit(TColumnShard& /*owner*/) {
        
        }

        void ResetStatusOnUpdate() {
            Status = {};
        }

        virtual TString DoDebugString() const = 0;

        std::optional<bool> StartedAsync;

    public:
        using TPtr = std::shared_ptr<ITransactionOperator>;
        using TFactory = NObjectFactory::TParametrizedObjectFactory<ITransactionOperator, NKikimrTxColumnShard::ETransactionKind, TTxInfo>;

        bool CheckTxInfoForReply(const TFullTxInfo& originalTxInfo) const {
            return DoCheckTxInfoForReply(originalTxInfo);
        }

        TString DebugString() const {
            return DoDebugString();
        }

        bool CheckAllowUpdate(const TFullTxInfo& currentTxInfo) const {
            return DoCheckAllowUpdate(currentTxInfo);
        }

        bool IsFail() const {
            return ProposeStartInfo && ProposeStartInfo->IsFail();
        }

        const TTxController::TProposeResult& GetProposeStartInfoVerified() const {
            AFL_VERIFY(!!ProposeStartInfo);
            return *ProposeStartInfo;
        }

        void SetProposeStartInfo(const TTxController::TProposeResult& info) {
            AFL_VERIFY(!ProposeStartInfo);
            ProposeStartInfo = info;
            if (IsFail()) {
                Status = EStatus::Failed;
            }
        }

        const TTxInfo& GetTxInfo() const {
            return TxInfo;
        }

        ITransactionOperator(const TTxInfo& txInfo)
            : TxInfo(txInfo)
        {}

        ui64 GetTxId() const {
            return TxInfo.TxId;
        }

        bool IsAsync() const {
            return DoIsAsync() && Status != EStatus::Failed && Status != EStatus::ReplySent;
        }

        virtual ~ITransactionOperator() {}

        virtual bool TxWithDeadline() const {
            return true;
        }

        bool Parse(TColumnShard& owner, const TString& data, const bool onLoad = false) {
            const bool result = DoParse(owner, data);
            if (!result) {
                AFL_VERIFY(!onLoad);
                ProposeStartInfo = TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Error processing commit TxId# " << TxInfo.TxId
                    << ". Parsing error");
                SwitchStateVerified(EStatus::Created, EStatus::Failed);
            } else {
                SwitchStateVerified(EStatus::Created, EStatus::Parsed);
            }
            if (onLoad) {
                ProposeStartInfo = TTxController::TProposeResult(NKikimrTxColumnShard::EResultStatus::PREPARED, "success on iteration before restart");
                Status = {};
            }
            return result;
        }

        void SendReply(TColumnShard& owner, const TActorContext& ctx) {
            AFL_VERIFY(!!ProposeStartInfo);
            if (ProposeStartInfo->IsFail()) {
                SwitchStateVerified(EStatus::Failed, EStatus::ReplySent);
            } else {
                SwitchStateVerified(EStatus::ProposeFinishedOnComplete, EStatus::ReplySent);
            }
            return DoSendReply(owner, ctx);
        }

        bool StartProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
            AFL_VERIFY(!ProposeStartInfo);
            AFL_VERIFY(!IsFail());
            ProposeStartInfo = DoStartProposeOnExecute(owner, txc);
            if (ProposeStartInfo->IsFail()) {
                SwitchStateVerified(EStatus::Parsed, EStatus::Failed);
            } else {
                SwitchStateVerified(EStatus::Parsed, EStatus::ProposeStartedOnExecute);
            }
            return !GetProposeStartInfoVerified().IsFail();
        }
        void StartProposeOnComplete(TColumnShard& owner, const TActorContext& ctx) {
            AFL_VERIFY(!IsFail());
            SwitchStateVerified(EStatus::ProposeStartedOnExecute, EStatus::ProposeStartedOnComplete);
            AFL_VERIFY(IsAsync());
            return DoStartProposeOnComplete(owner, ctx);
        }
        void FinishProposeOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
            AFL_VERIFY(!IsFail());
            SwitchStateVerified(EStatus::ProposeStartedOnComplete, EStatus::ProposeFinishedOnExecute);
            AFL_VERIFY(IsAsync() || StartedAsync);
            return DoFinishProposeOnExecute(owner, txc);
        }
        void FinishProposeOnComplete(TColumnShard& owner, const TActorContext& ctx) {
            if (IsFail()) {
                AFL_VERIFY(Status == EStatus::Failed);
            } else if (IsAsync() || StartedAsync) {
                SwitchStateVerified(EStatus::ProposeFinishedOnExecute, EStatus::ProposeFinishedOnComplete);
            } else {
                SwitchStateVerified(EStatus::ProposeStartedOnExecute, EStatus::ProposeFinishedOnComplete);
            }
            return DoFinishProposeOnComplete(owner, ctx);
        }

        virtual bool ProgressOnExecute(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) = 0;

        virtual bool ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) = 0;
        virtual bool CompleteOnAbort(TColumnShard& owner, const TActorContext& ctx) = 0;

        virtual void RegisterSubscriber(const TActorId&) {
            AFL_VERIFY(false)("message", "Not implemented");
        };
        void OnTabletInit(TColumnShard& owner) {
            AFL_VERIFY(!StartedAsync);
            StartedAsync = true;
            DoOnTabletInit(owner);
        }
    };

private:
    const TDuration MaxCommitTxDelay = TDuration::Seconds(30);
    TColumnShard& Owner;
    std::set<TPlanQueueItem> DeadlineQueue;
    std::set<TPlanQueueItem> PlanQueue;
    std::set<TPlanQueueItem> RunningQueue;

    THashMap<ui64, ITransactionOperator::TPtr> Operators;

private:
    ui64 GetAllowedStep() const;
    bool AbortTx(const TPlanQueueItem planQueueItem, NTabletFlatExecutor::TTransactionContext& txc);

    TTxInfo RegisterTx(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc);
    TTxInfo RegisterTxWithDeadline(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc);
    bool StartedFlag = false;
public:
    TTxController(TColumnShard& owner);

    ITransactionOperator::TPtr GetTxOperator(const ui64 txId) const;
    ITransactionOperator::TPtr GetVerifiedTxOperator(const ui64 txId) const;

    ui64 GetMemoryUsage() const;
    bool HaveOutdatedTxs() const;

    bool Load(NTabletFlatExecutor::TTransactionContext& txc);

    [[nodiscard]] std::shared_ptr<TTxController::ITransactionOperator> UpdateTxSourceInfo(const TFullTxInfo& tx, NTabletFlatExecutor::TTransactionContext& txc);

    [[nodiscard]] std::shared_ptr<TTxController::ITransactionOperator> StartProposeOnExecute(
        const TTxController::TTxInfo& txInfo, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc);
    void StartProposeOnComplete(const ui64 txId, const TActorContext& ctx);

    void FinishProposeOnExecute(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

    void FinishProposeOnComplete(const ui64 txId, const TActorContext& ctx);

    bool ExecuteOnCancel(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    bool CompleteOnCancel(const ui64 txId, const TActorContext& ctx);

    std::optional<TTxInfo> StartPlannedTx();
    void FinishPlannedTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);
    void CompleteRunningTx(const TPlanQueueItem& tx);

    std::optional<TPlanQueueItem> GetPlannedTx() const;
    TPlanQueueItem GetFrontTx() const;
    std::optional<TTxInfo> GetTxInfo(const ui64 txId) const;
    TTxInfo GetTxInfoVerified(const ui64 txId) const;
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

