#include "tx_controller.h"
#include "transactions/tx_finish_async.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

TTxController::TTxController(TColumnShard& owner)
    : Owner(owner) {
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
    return Operators.size() * (sizeof(TTxController::ITransactionOperator) + 24) + DeadlineQueue.size() * sizeof(TPlanQueueItem) +
           (PlanQueue.size() + RunningQueue.size()) * sizeof(TPlanQueueItem);
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

    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        const NKikimrTxColumnShard::ETransactionKind txKind = rowset.GetValue<Schema::TxInfo::TxKind>();
        ITransactionOperator::TPtr txOperator(ITransactionOperator::TFactory::Construct(txKind, TTxInfo(txKind, txId)));
        Y_ABORT_UNLESS(!!txOperator);
        const TString txBody = rowset.GetValue<Schema::TxInfo::TxBody>();
        Y_ABORT_UNLESS(txOperator->Parse(Owner, txBody, true));

        auto& txInfo = txOperator->MutableTxInfo();
        txInfo.MaxStep = rowset.GetValue<Schema::TxInfo::MaxStep>();
        if (txInfo.MaxStep != Max<ui64>()) {
            txInfo.MinStep = txInfo.MaxStep - MaxCommitTxDelay.MilliSeconds();
        }
        txInfo.PlanStep = rowset.GetValueOrDefault<Schema::TxInfo::PlanStep>(0);
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
    return true;
}

TTxController::ITransactionOperator::TPtr TTxController::GetTxOperator(const ui64 txId) const {
    auto it = Operators.find(txId);
    if (it == Operators.end()) {
        return nullptr;
    }
    return it->second;
}

TTxController::ITransactionOperator::TPtr TTxController::GetVerifiedTxOperator(const ui64 txId) const {
    auto it = Operators.find(txId);
    AFL_VERIFY(it != Operators.end())("tx_id", txId);
    return it->second;
}

std::shared_ptr<TTxController::ITransactionOperator> TTxController::UpdateTxSourceInfo(const TFullTxInfo& tx, NTabletFlatExecutor::TTransactionContext& txc) {
    auto op = GetVerifiedTxOperator(tx.GetTxId());
    op->ResetStatusOnUpdate();
    auto& txInfo = op->MutableTxInfo();
    txInfo.Source = tx.Source;
    txInfo.Cookie = tx.Cookie;
    txInfo.SeqNo = tx.SeqNo;

    NIceDb::TNiceDb db(txc.DB);
    Schema::UpdateTxInfoSource(db, txInfo);
    return op;
}

TTxController::TTxInfo TTxController::RegisterTx(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    auto& txInfo = txOperator->GetTxInfo();
    AFL_VERIFY(txInfo.MaxStep == Max<ui64>());
    AFL_VERIFY(Operators.emplace(txInfo.TxId, txOperator).second);

    Schema::SaveTxInfo(db, txInfo, txBody);
    return txInfo;
}

TTxController::TTxInfo TTxController::RegisterTxWithDeadline(const std::shared_ptr<TTxController::ITransactionOperator>& txOperator, const TString& txBody, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto& txInfo = txOperator->MutableTxInfo();
    txInfo.MinStep = GetAllowedStep();
    txInfo.MaxStep = txInfo.MinStep + MaxCommitTxDelay.MilliSeconds();

    AFL_VERIFY(Operators.emplace(txOperator->GetTxId(), txOperator).second);

    Schema::SaveTxInfo(db, txInfo, txBody);
    DeadlineQueue.emplace(txInfo.MaxStep, txOperator->GetTxId());
    return txInfo;
}

bool TTxController::AbortTx(const TPlanQueueItem planQueueItem, NTabletFlatExecutor::TTransactionContext& txc) {
    auto opIt = Operators.find(planQueueItem.TxId);
    Y_ABORT_UNLESS(opIt != Operators.end());
    Y_ABORT_UNLESS(opIt->second->GetTxInfo().PlanStep == 0);
    opIt->second->ExecuteOnAbort(Owner, txc);
    opIt->second->CompleteOnAbort(Owner, NActors::TActivationContext::AsActorContext());

    AFL_VERIFY(Operators.erase(planQueueItem.TxId));
    AFL_VERIFY(DeadlineQueue.erase(planQueueItem));
    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, planQueueItem.TxId);
    return true;
}

bool TTxController::CompleteOnCancel(const ui64 txId, const TActorContext& ctx) {
    auto opIt = Operators.find(txId);
    if (opIt == Operators.end()) {
        return true;
    }
    Y_ABORT_UNLESS(opIt != Operators.end());
    if (opIt->second->GetTxInfo().PlanStep != 0) {
        return false;
    }
    opIt->second->CompleteOnAbort(Owner, ctx);

    if (opIt->second->GetTxInfo().MaxStep != Max<ui64>()) {
        DeadlineQueue.erase(TPlanQueueItem(opIt->second->GetTxInfo().MaxStep, txId));
    }
    Operators.erase(txId);
    return true;
}

bool TTxController::ExecuteOnCancel(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto opIt = Operators.find(txId);
    if (opIt == Operators.end()) {
        return true;
    }
    Y_ABORT_UNLESS(opIt != Operators.end());
    if (opIt->second->GetTxInfo().PlanStep != 0) {
        return false;
    }

    opIt->second->ExecuteOnAbort(Owner, txc);

    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, txId);
    return true;
}

std::optional<TTxController::TTxInfo> TTxController::StartPlannedTx() {
    if (!PlanQueue.empty()) {
        auto node = PlanQueue.extract(PlanQueue.begin());
        auto& item = node.value();
        TPlanQueueItem tx(item.Step, item.TxId);
        RunningQueue.emplace(std::move(item));
        return GetTxInfoVerified(item.TxId);
    }
    return std::nullopt;
}

void TTxController::FinishPlannedTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, txId);
}

void TTxController::CompleteRunningTx(const TPlanQueueItem& txItem) {
    AFL_VERIFY(Operators.erase(txItem.TxId));
    AFL_VERIFY(RunningQueue.erase(txItem))("info", txItem.DebugString());
}

std::optional<TTxController::TPlanQueueItem> TTxController::GetPlannedTx() const {
    if (PlanQueue.empty()) {
        return std::nullopt;
    }
    return *PlanQueue.begin();
}

std::optional<TTxController::TTxInfo> TTxController::GetTxInfo(const ui64 txId) const {
    auto it = Operators.find(txId);
    if (it != Operators.end()) {
        return it->second->GetTxInfo();
    }
    return std::nullopt;
}

TTxController::TTxInfo TTxController::GetTxInfoVerified(const ui64 txId) const {
    auto it = Operators.find(txId);
    AFL_VERIFY(it != Operators.end());
    return it->second->GetTxInfo();
}

NEvents::TDataEvents::TCoordinatorInfo TTxController::BuildCoordinatorInfo(const TTxInfo& txInfo) const {
    if (Owner.ProcessingParams) {
        return NEvents::TDataEvents::TCoordinatorInfo(txInfo.MinStep, txInfo.MaxStep, Owner.ProcessingParams->GetCoordinators());
    }
    return NEvents::TDataEvents::TCoordinatorInfo(txInfo.MinStep, txInfo.MaxStep, {});
}

size_t TTxController::CleanExpiredTxs(NTabletFlatExecutor::TTransactionContext& txc) {
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
            AbortTx(*it, txc);
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
        return EPlanResult::Skipped;
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
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("method", "TTxController::StartProposeOnExecute")(
        "tx_info", txInfo.DebugString())("tx_info", txInfo.DebugString());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start");
    std::shared_ptr<TTxController::ITransactionOperator> txOperator(TTxController::ITransactionOperator::TFactory::Construct(txInfo.TxKind, txInfo));
    AFL_VERIFY(!!txOperator);
    if (!txOperator->Parse(Owner, txBody)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse txOperator");
        return txOperator;
    }

    auto txInfoPtr = GetTxInfo(txInfo.TxId);
    if (!!txInfoPtr) {
        if (!txOperator->CheckAllowUpdate(*txInfoPtr)) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "incorrect duplication")("actual_tx", txInfoPtr->DebugString());
            TTxController::TProposeResult proposeResult(
                NKikimrTxColumnShard::EResultStatus::ERROR, TStringBuilder() << "Another commit TxId# " << txInfo.TxId << " has already been proposed");
            txOperator->SetProposeStartInfo(proposeResult);
            return txOperator;
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "update duplication data")("deprecated_tx", txInfoPtr->DebugString());
            return UpdateTxSourceInfo(txOperator->GetTxInfo(), txc);
        }
    } else {
        if (txOperator->StartProposeOnExecute(Owner, txc)) {
            if (txOperator->TxWithDeadline()) {
                RegisterTxWithDeadline(txOperator, txBody, txc);
            } else {
                RegisterTx(txOperator, txBody, txc);
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "registered");
        } else {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "problem on start")(
                "message", txOperator->GetProposeStartInfoVerified().GetStatusMessage());
        }
        return txOperator;
    }
}

void TTxController::StartProposeOnComplete(const ui64 txId, const TActorContext& ctx) {
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("method", "TTxController::StartProposeOnComplete")("tx_id", txId);
    auto txOperator = GetTxOperator(txId);
    if (!txOperator) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "cannot found txOperator in propose transaction base")("tx_id", txId);
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start");
        txOperator->StartProposeOnComplete(Owner, ctx);
    }
}

void TTxController::FinishProposeOnExecute(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("method", "TTxController::FinishProposeOnExecute")("tx_id", txId);
    auto txOperator = GetTxOperator(txId);
    if (!txOperator) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "cannot found txOperator in propose transaction base")("tx_id", txId);
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start");
        txOperator->FinishProposeOnExecute(Owner, txc);
    }
}

void TTxController::FinishProposeOnComplete(const ui64 txId, const TActorContext& ctx) {
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("method", "TTxController::FinishProposeOnComplete")("tx_id", txId);
    auto txOperator = GetTxOperator(txId);
    if (!txOperator) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("error", "cannot found txOperator in propose transaction finish")("tx_id", txId);
        return;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "start")("tx_info", txOperator->GetTxInfo().DebugString());
    TTxController::TProposeResult proposeResult = txOperator->GetProposeStartInfoVerified();
    AFL_VERIFY(!txOperator->IsFail());
    txOperator->FinishProposeOnComplete(Owner, ctx);
    txOperator->SendReply(Owner, ctx);
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
