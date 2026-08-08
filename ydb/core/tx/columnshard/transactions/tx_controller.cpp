#include "tx_controller.h"

#include "transactions/tx_finish_async.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/subscriber/events/tx_completed/event.h>

namespace NKikimr::NColumnShard {

TTxController::TTxController(TColumnShard& owner)
    : Owner(owner)
    , Counters(owner.Counters.GetCSCounters().TxProgress)
{
}

bool TTxController::HaveOutdatedTxs() const {
    if (DeadlineQueue.empty()) {
        return false;
    }
    ui64 step = Owner.GetOutdatedStep();
    auto it = DeadlineQueue.begin();
    // Return true if the first transaction has no chance to be planned
    return it->Step <= step;
}

ui64 TTxController::GetAllowedStep() const {
    return Max(Owner.GetOutdatedStep() + 1, TAppData::TimeProvider->Now().MilliSeconds());
}

ui64 TTxController::GetMemoryUsage() const {
    return (Operators.size() + CompletingOperators.size()) * (sizeof(TTxController::ITransactionOperator) + 24) +
           DeadlineQueue.size() * sizeof(TPlanQueueItem) + (PlanQueue.size() + RunningQueue.size()) * sizeof(TPlanQueueItem);
}

TTxController::TPlanQueueItem TTxController::GetFrontTx() const {
    if (!RunningQueue.empty()) {
        return TPlanQueueItem(RunningQueue.begin()->Step, RunningQueue.begin()->TxId);
    } else if (!PlanQueue.empty()) {
        return TPlanQueueItem(PlanQueue.begin()->Step, PlanQueue.begin()->TxId);
    }
    return TPlanQueueItem(Owner.LastPlannedStep, 0);
}

bool TTxController::Load(NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto rowset = db.Table<Schema::TxInfo>().GreaterOrEqual(0).Select();
    if (!rowset.IsReady()) {
        return false;
    }

    ui32 countWithDeadline = 0;
    ui32 countOverrideDeadline = 0;
    ui32 countNoDeadline = 0;
    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        const NKikimrTxColumnShard::ETransactionKind txKind = rowset.GetValue<Schema::TxInfo::TxKind>();
        ITransactionOperator::TPtr txOperator(ITransactionOperator::TFactory::Construct(txKind, TTxInfo(txKind, txId)));
        AFL_VERIFY(!!txOperator)("kind", txKind);
        const TString txBody = rowset.GetValue<Schema::TxInfo::TxBody>();
        AFL_VERIFY(txOperator->Parse(Owner, txBody, true));

        auto& txInfo = txOperator->MutableTxInfo();
        txInfo.MaxStep = rowset.GetValue<Schema::TxInfo::MaxStep>();
        if (txInfo.MaxStep != Max<ui64>()) {
            txInfo.MinStep = txInfo.MaxStep - MaxCommitTxDelay.MilliSeconds();
            ++countWithDeadline;
        } else if (txOperator->TxWithDeadline()) {
            txInfo.MinStep = GetAllowedStep();
            txInfo.MaxStep = txInfo.MinStep + MaxCommitTxDelay.MilliSeconds();
            ++countOverrideDeadline;
        } else {
            // For transactions without deadline (e.g., schema transactions),
            // use GetAllowedStep() to ensure MinStep is not zero after restart
            txInfo.MinStep = GetAllowedStep();
            ++countNoDeadline;
        }
        txInfo.PlanStep = rowset.GetValueOrDefault<Schema::TxInfo::PlanStep>(0);
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_TX)("event", "Load")("tx_id", txInfo.TxId)("plan_step", txInfo.PlanStep);
        txInfo.Source = rowset.GetValue<Schema::TxInfo::Source>();
        txInfo.Cookie = rowset.GetValue<Schema::TxInfo::Cookie>();
        txInfo.DeserializeSeqNoFromString(rowset.GetValue<Schema::TxInfo::SeqNo>());

        if (txInfo.PlanStep != 0) {
            PlanQueue.emplace(txInfo.PlanStep, txInfo.TxId);
        } else if (txInfo.MaxStep != Max<ui64>()) {
            DeadlineQueue.emplace(txInfo.MaxStep, txInfo.TxId);
        }
        AFL_VERIFY(Operators.emplace(txId, txOperator).second);

        if (!rowset.Next()) {
            return false;
        }
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_TX)("override", countOverrideDeadline)("no_dl", countNoDeadline)("dl", countWithDeadline)(
        "operators", Operators.size())("plan", PlanQueue.size())("dl_queue", DeadlineQueue.size());
    return true;
}

std::shared_ptr<TTxController::ITransactionOperator> TTxController::UpdateTxSourceInfo(
    const TFullTxInfo& tx, NTabletFlatExecutor::TTransactionContext& txc) {
<<<<<<< HEAD
    auto op = GetTxOperatorVerified(tx.GetTxId());
    op->ResetStatusOnUpdate();
=======
    auto op = GetTxOperator(tx.GetTxId(), ETxOperatorStatus::InProgress);
    const bool sourceChanged = op->GetTxInfo().Source != tx.Source;
    op->ResetStatusOnUpdate(sourceChanged);

>>>>>>> 5427ca9d9ef (fix TxProgress enqueue (#43594))
    auto& txInfo = op->MutableTxInfo();
    txInfo.Source = tx.Source;
    txInfo.MinStep = tx.MinStep;
    txInfo.MaxStep = tx.MaxStep;
    txInfo.Cookie = tx.Cookie;
    txInfo.SeqNo = tx.SeqNo;

    NIceDb::TNiceDb db(txc.DB);
    Schema::UpdateTxInfoSource(db, txInfo);
    return op;
}

TTxController::TTxInfo TTxController::RegisterTx(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator, const TString& txBody,
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    auto& txInfo = txOperator->GetTxInfo();
    AFL_VERIFY(txInfo.MaxStep == Max<ui64>());
    AFL_VERIFY(Operators.emplace(txInfo.TxId, txOperator).second);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_TX)("event", "RegisterTx")("tx_id", txInfo.TxId)("plan_step", txInfo.PlanStep);
    Schema::SaveTxInfo(db, txInfo, txBody);
    Counters.OnRegisterTx(txOperator->GetOpType());
    return txInfo;
}

TTxController::TTxInfo TTxController::RegisterTxWithDeadline(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator,
    const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto& txInfo = txOperator->MutableTxInfo();
    txInfo.MinStep = GetAllowedStep();
    txInfo.MaxStep = txInfo.MinStep + MaxCommitTxDelay.MilliSeconds();

    AFL_VERIFY(Operators.emplace(txOperator->GetTxId(), txOperator).second);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_TX)("event", "RegisterTxWithDeadline")("tx_id", txInfo.TxId)("plan_step", txInfo.PlanStep);
    Schema::SaveTxInfo(db, txInfo, txBody);
    DeadlineQueue.emplace(txInfo.MaxStep, txOperator->GetTxId());
    Counters.OnRegisterTx(txOperator->GetOpType());
    return txInfo;
}

bool TTxController::AbortTx(const TPlanQueueItem planQueueItem) {
    auto opIt = Operators.find(planQueueItem.TxId);
    AFL_VERIFY(opIt != Operators.end())("tx_id", planQueueItem.TxId);
    AFL_VERIFY(opIt->second->GetTxInfo().PlanStep == 0)("tx_id", planQueueItem.TxId)("plan_step", opIt->second->GetTxInfo().PlanStep);
    AFL_VERIFY(DeadlineQueue.erase(planQueueItem))("tx_id", planQueueItem.TxId);
    Counters.OnAbortTx(opIt->second->GetOpType());
    Owner.CancelTransaction(planQueueItem.TxId);
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "abort_tx")("tx_id", planQueueItem.TxId);
    return true;
}

bool TTxController::ExecuteOnCancel(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    ITransactionOperator::TPtr op = MoveOperatorToCompleting(txId);
    AFL_VERIFY(op->GetTxInfo().PlanStep == 0)("tx_id", txId)("plan_step", op->GetTxInfo().PlanStep);

    op->ExecuteOnAbort(Owner, txc);

    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, txId);
    return true;
}

bool TTxController::CompleteOnCancel(const ui64 txId, const TActorContext& ctx) {
    auto opIt = CompletingOperators.find(txId);
    AFL_VERIFY(opIt != CompletingOperators.end())("tx_id", txId);
    AFL_VERIFY(opIt->second->GetTxInfo().PlanStep == 0)("tx_id", txId)("plan_step", opIt->second->GetTxInfo().PlanStep);

    opIt->second->CompleteOnAbort(Owner, ctx);

    if (opIt->second->GetTxInfo().MaxStep != Max<ui64>()) {
        DeadlineQueue.erase(TPlanQueueItem(opIt->second->GetTxInfo().MaxStep, txId));
    }
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "cancel_tx")("tx_id", txId);
    OnTxCompleted(txId);
    return true;
}

std::optional<TTxController::TTxInfo> TTxController::GetFirstPlannedTx() const {
    if (!PlanQueue.empty()) {
        return GetTxInfoVerified(PlanQueue.begin()->TxId, ETxOperatorStatus::InProgress);
    }
    return std::nullopt;
}

std::optional<TTxController::TTxInfo> TTxController::PopFirstPlannedTx() {
    if (!PlanQueue.empty()) {
        auto node = PlanQueue.extract(PlanQueue.begin());
        auto& item = node.value();
        auto txId = item.TxId;
        TPlanQueueItem tx(item.Step, item.TxId);
        RunningQueue.emplace(std::move(item));
        return GetTxInfoVerified(txId, ETxOperatorStatus::InProgress);
    }
    return std::nullopt;
}

void TTxController::ProgressOnExecute(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    ITransactionOperator::TPtr op = MoveOperatorToCompleting(txId);
    NIceDb::TNiceDb db(txc.DB);
    Counters.OnFinishPlannedTx(op->GetOpType());
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD_TX)("event", "finished_tx")("tx_id", txId);
    Schema::EraseTxInfo(db, txId);
}

void TTxController::ProgressOnComplete(const TPlanQueueItem& txItem) {
    OnTxCompleted(txItem.TxId);
    AFL_VERIFY(RunningQueue.erase(txItem))("info", txItem.DebugString());
}

TTxController::ITransactionOperator::TPtr TTxController::MoveOperatorToCompleting(const ui64 txId) {
    auto opIt = Operators.find(txId);
    AFL_VERIFY(opIt != Operators.end())("tx_id", txId);

    ITransactionOperator::TPtr op = std::move(opIt->second);
    Operators.erase(opIt);

    AFL_VERIFY(CompletingOperators.emplace(txId, op).second)("tx_id", txId);
    return op;
}

void TTxController::OnTxCompleted(const ui64 txId) {
    AFL_VERIFY(CompletingOperators.erase(txId))("tx_id", txId);
    Owner.Subscribers->OnEvent(std::make_shared<NColumnShard::NSubscriber::TEventTxCompleted>(txId));
}

void TTxController::WriteTxOperatorInfo(NTabletFlatExecutor::TTransactionContext& txc, const ui64 txId, const TString& data) {
    AFL_VERIFY(GetTxOperator(txId, ETxOperatorStatus::InProgress))("tx_id", txId);
    NIceDb::TNiceDb db(txc.DB);
    NColumnShard::Schema::UpdateTxInfoBody(db, txId, data);
}

THashSet<ui64> TTxController::GetTxs() const {
    THashSet<ui64> result;
    for (const auto& [txId, _] : Operators) {
        result.emplace(txId);
    }
    return result;
}

std::optional<TTxController::TPlanQueueItem> TTxController::GetPlannedTx() const {
    if (PlanQueue.empty()) {
        return std::nullopt;
    }
    return *PlanQueue.begin();
}

TTxController::ITransactionOperator::TPtr TTxController::GetTxOperator(const ui64 txId, ETxOperatorStatus status, const bool optional) const {
    ITransactionOperator::TPtr result;
    switch (status) {
        case ETxOperatorStatus::InProgress: {
            auto it = Operators.find(txId);
            if (it != Operators.end()) {
                result = it->second;
            }
            break;
        }
        case ETxOperatorStatus::Completing: {
            auto it = CompletingOperators.find(txId);
            if (it != CompletingOperators.end()) {
                result = it->second;
            }
            break;
        }
        case ETxOperatorStatus::Any: {
            auto it = Operators.find(txId);
            if (it != Operators.end()) {
                result = it->second;
            }
            if (result == nullptr) {
                auto it = CompletingOperators.find(txId);
                if (it != CompletingOperators.end()) {
                    result = it->second;
                }
            }
            break;
        }
        default:
            AFL_VERIFY(false)("status", status);
    }
    AFL_VERIFY(optional || result)("tx_id", txId)("status", status);
    return result;
}

std::optional<TTxController::TTxInfo> TTxController::GetTxInfo(const ui64 txId, ETxOperatorStatus status) const {
    auto op = GetTxOperator(txId, status, /*optional*/ true);
    if (!op) {
        return std::nullopt;
    }
    return op->GetTxInfo();
}

TTxController::TTxInfo TTxController::GetTxInfoVerified(const ui64 txId, ETxOperatorStatus status) const {
    return GetTxOperator(txId, status)->GetTxInfo();
}

NEvents::TDataEvents::TCoordinatorInfo TTxController::BuildCoordinatorInfo(const TTxInfo& txInfo) const {
    if (Owner.ProcessingParams) {
        return NEvents::TDataEvents::TCoordinatorInfo(txInfo.MinStep, txInfo.MaxStep, Owner.ProcessingParams->GetCoordinators());
    }
    return NEvents::TDataEvents::TCoordinatorInfo(txInfo.MinStep, txInfo.MaxStep, {});
}

size_t TTxController::CleanExpiredTxs() {
    size_t removedCount = 0;
    if (HaveOutdatedTxs()) {
        ui64 outdatedStep = Owner.GetOutdatedStep();
        while (DeadlineQueue.size()) {
            auto it = DeadlineQueue.begin();
            if (outdatedStep < it->Step) {
                // This transaction has a chance to be planned
                break;
            }
            ui64 txId = it->TxId;
            LOG_S_DEBUG(TStringBuilder() << "Removing outdated txId " << txId << " max step " << it->Step << " outdated step ");
            AbortTx(*it);
            ++removedCount;
        }
    }
    return removedCount;
}

TDuration TTxController::GetTxCompleteLag(ui64 timecastStep) const {
    if (PlanQueue.empty()) {
        return TDuration::Zero();
    }

    ui64 currentStep = PlanQueue.begin()->Step;
    if (timecastStep > currentStep) {
        return TDuration::MilliSeconds(timecastStep - currentStep);
    }

    return TDuration::Zero();
}

TTxController::EPlanResult TTxController::PlanTx(const ui64 planStep, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto it = Operators.find(txId);
    if (it == Operators.end()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("event", "skip_plan_tx")("tx_id", txId);
        return EPlanResult::Skipped;
    } else {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_TX)("event", "plan_tx")("tx_id", txId)("plan_step", it->second->MutableTxInfo().PlanStep);
    }
    auto& txInfo = it->second->MutableTxInfo();
    if (txInfo.PlanStep == 0) {
        txInfo.PlanStep = planStep;
        NIceDb::TNiceDb db(txc.DB);
        Schema::UpdateTxInfoPlanStep(db, txId, planStep);
        PlanQueue.emplace(planStep, txId);
        if (txInfo.MaxStep != Max<ui64>()) {
            DeadlineQueue.erase(TPlanQueueItem(txInfo.MaxStep, txId));
        }
        return EPlanResult::Planned;
    } else {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_TX)("event", "skip_plan_tx_plan_step_is_not_zero")("tx_id", txId)("plan_step", txInfo.PlanStep)(
            "schemeshard_plan_step", planStep);
    }
    return EPlanResult::AlreadyPlanned;
}

void TTxController::OnTabletInit() {
    AFL_VERIFY(!StartedFlag);
    StartedFlag = true;
    for (auto&& txOperator : Operators) {
        txOperator.second->OnTabletInit(Owner);
    }
}

std::shared_ptr<TTxController::ITransactionOperator> TTxController::StartProposeOnExecute(
    const TTxController::TTxInfo& txInfo, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc) {
    NActors::TLogContextGuard lGuard =
        NActors::TLogContextBuilder::Build()("method", "TTxController::StartProposeOnExecute")("tx_info", txInfo.DebugString());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "start");
    std::shared_ptr<TTxController::ITransactionOperator> txOperator(
        TTxController::ITransactionOperator::TFactory::Construct(txInfo.TxKind, txInfo));
    AFL_VERIFY(!!txOperator);
    if (!txOperator->Parse(Owner, txBody)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("error", "cannot parse txOperator");
        return txOperator;
    }
    Counters.OnStartProposeOnExecute(txOperator->GetOpType());

    auto txInfoPtr = GetTxInfo(txInfo.TxId, ETxOperatorStatus::Any);
    if (txInfoPtr.has_value()) {
        if (IsTxCompleting(txInfo.TxId) || !txOperator->CheckAllowUpdate(*txInfoPtr)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("error", "incorrect duplication")("actual_tx", txInfoPtr->DebugString());
            TTxController::TProposeResult proposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
                TStringBuilder() << "Another commit TxId# " << txInfo.TxId << " has already been proposed");
            txOperator->SetProposeStartInfo(proposeResult);
            return txOperator;
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("error", "update duplication data")("deprecated_tx", txInfoPtr->DebugString());
            return UpdateTxSourceInfo(txOperator->GetTxInfo(), txc);
        }
    } else {
        if (txOperator->StartProposeOnExecute(Owner, txc)) {
            if (txOperator->TxWithDeadline()) {
                RegisterTxWithDeadline(txOperator, txBody, txc);
            } else {
                RegisterTx(txOperator, txBody, txc);
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "registered")("tx_operator", txOperator->GetOpType());
        } else {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)("error", "problem on start")(
                "message", txOperator->GetProposeStartInfoVerified().GetStatusMessage());
        }
        return txOperator;
    }
}

void TTxController::StartProposeOnComplete(ITransactionOperator& txOperator, const TActorContext& ctx) {
    NActors::TLogContextGuard lGuard =
        NActors::TLogContextBuilder::Build()("method", "TTxController::StartProposeOnComplete")("tx_id", txOperator.GetTxId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "start");
    txOperator.StartProposeOnComplete(Owner, ctx);
    Counters.OnStartProposeOnComplete(txOperator.GetOpType());
}

void TTxController::FinishProposeOnExecute(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("method", "TTxController::FinishProposeOnExecute")("tx_id", txId);
    if (auto txOperator = GetTxOperator(txId, ETxOperatorStatus::InProgress, /*optional*/ true)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "start");
        txOperator->FinishProposeOnExecute(Owner, txc);
        Counters.OnFinishProposeOnExecute(txOperator->GetOpType());
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("error", "cannot found txOperator in propose transaction base")("tx_id", txId);
    }
}

void TTxController::FinishProposeOnComplete(ITransactionOperator& txOperator, const TActorContext& ctx) {
    NActors::TLogContextGuard lGuard =
        NActors::TLogContextBuilder::Build()("method", "TTxController::FinishProposeOnComplete")("tx_id", txOperator.GetTxId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)("event", "start")("tx_info", txOperator.GetTxInfo().DebugString());
    TTxController::TProposeResult proposeResult = txOperator.GetProposeStartInfoVerified();
    AFL_VERIFY(!txOperator.IsFail());
    txOperator.FinishProposeOnComplete(Owner, ctx);
    txOperator.SendReply(Owner, ctx);
    Counters.OnFinishProposeOnComplete(txOperator.GetOpType());
}

void TTxController::FinishProposeOnComplete(const ui64 txId, const TActorContext& ctx) {
    auto txOperator = GetTxOperator(txId, ETxOperatorStatus::InProgress, /*optional*/ true);
    if (!txOperator) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_TX)("error", "cannot found txOperator in propose transaction finish")("tx_id", txId);
        return;
    }
    return FinishProposeOnComplete(*txOperator, ctx);
}

void TTxController::ITransactionOperator::SwitchStateVerified(const EStatus from, const EStatus to) {
    AFL_VERIFY(!Status || *Status == from)("error", "incorrect expected status")("real_state", *Status)("expected", from)("details", DebugString());
    Status = to;
}

}   // namespace NKikimr::NColumnShard

template <>
void Out<NKikimrTxColumnShard::ETransactionKind>(IOutputStream& out, TTypeTraits<NKikimrTxColumnShard::ETransactionKind>::TFuncParam txKind) {
    out << (ui64)txKind;
}
