#include "tx_controller.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>


namespace NKikimr::NColumnShard {

TTxController::TTxController(TColumnShard& owner)
    : Owner(owner)
{}

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
    return  BasicTxInfo.size() * sizeof(TTxInfo) +
            DeadlineQueue.size() * sizeof(TPlanQueueItem) +
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
    if (!rowset.IsReady())
        return false;

    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        const NKikimrTxColumnShard::ETransactionKind txKind = rowset.GetValue<Schema::TxInfo::TxKind>();
        
        auto txInfoIt = BasicTxInfo.emplace(txId, TTxInfo(txKind, txId)).first;
        auto& txInfo = txInfoIt->second;
        txInfo.MaxStep = rowset.GetValue<Schema::TxInfo::MaxStep>();
        if (txInfo.MaxStep != Max<ui64>()) {
            txInfo.MinStep = txInfo.MaxStep - MaxCommitTxDelay.MilliSeconds();
        }
        txInfo.PlanStep = rowset.GetValueOrDefault<Schema::TxInfo::PlanStep>(0);
        txInfo.Source = rowset.GetValue<Schema::TxInfo::Source>();
        txInfo.Cookie = rowset.GetValue<Schema::TxInfo::Cookie>();

        if (txInfo.PlanStep != 0) {
            PlanQueue.emplace(txInfo.PlanStep, txInfo.TxId);
        } else if (txInfo.MaxStep != Max<ui64>()) {
            DeadlineQueue.emplace(txInfo.MaxStep, txInfo.TxId);
        }

        const TString txBody = rowset.GetValue<Schema::TxInfo::TxBody>();
        ITransactionOperatior::TPtr txOperator(ITransactionOperatior::TFactory::Construct(txInfo.TxKind, txInfo));
        Y_ABORT_UNLESS(!!txOperator);
        Y_ABORT_UNLESS(txOperator->Parse(txBody));
        Operators[txId] = txOperator;

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

TTxController::ITransactionOperatior::TPtr TTxController::GetTxOperator(const ui64 txId) {
    auto it = Operators.find(txId);
    if(it == Operators.end()) {
        return nullptr;
    }
    return it->second;
}

TTxController::ITransactionOperatior::TPtr TTxController::GetVerifiedTxOperator(const ui64 txId) {
    auto it = Operators.find(txId);
    AFL_VERIFY(it != Operators.end())("tx_id", txId);
    return it->second;
}

TTxController::TTxInfo TTxController::RegisterTx(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto txInfoIt = BasicTxInfo.emplace(txId, TTxInfo(txKind, txId)).first;
    auto& txInfo = txInfoIt->second;
    txInfo.Source = source;
    txInfo.Cookie = cookie;

    ITransactionOperatior::TPtr txOperator(ITransactionOperatior::TFactory::Construct(txInfo.TxKind, txInfo));
    Y_ABORT_UNLESS(!!txOperator);
    Y_ABORT_UNLESS(txOperator->Parse(txBody));
    Operators[txId] = txOperator;

    Schema::SaveTxInfo(db, txInfo.TxId, txInfo.TxKind, txBody, Max<ui64>(), txInfo.Source, txInfo.Cookie);
    return txInfo;
}

TTxController::TTxInfo TTxController::RegisterTxWithDeadline(const ui64 txId, const NKikimrTxColumnShard::ETransactionKind& txKind, const TString& txBody, const TActorId& source, const ui64 cookie, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto txInfoIt = BasicTxInfo.emplace(txId, TTxInfo(txKind, txId)).first;
    auto& txInfo = txInfoIt->second;
    txInfo.Source = source;
    txInfo.Cookie = cookie;
    txInfo.MinStep = GetAllowedStep();
    txInfo.MaxStep = txInfo.MinStep + MaxCommitTxDelay.MilliSeconds();

    ITransactionOperatior::TPtr txOperator(ITransactionOperatior::TFactory::Construct(txInfo.TxKind, txInfo));
    Y_ABORT_UNLESS(!!txOperator);
    Y_ABORT_UNLESS(txOperator->Parse(txBody));
    Operators[txId] = txOperator;

    Schema::SaveTxInfo(db, txInfo.TxId, txInfo.TxKind, txBody, txInfo.MaxStep, txInfo.Source, txInfo.Cookie);
    DeadlineQueue.emplace(txInfo.MaxStep, txId);
    return txInfo;
}

bool TTxController::AbortTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto it = BasicTxInfo.find(txId);
    if (it == BasicTxInfo.end()) {
        return true;
    }
    Y_ABORT_UNLESS(it->second.PlanStep == 0);

    auto opIt = Operators.find(txId);
    Y_ABORT_UNLESS(opIt != Operators.end());
    opIt->second->Abort(Owner, txc);

    if (it->second.MaxStep != Max<ui64>()) {
        DeadlineQueue.erase(TPlanQueueItem(it->second.MaxStep, txId));
    }
    BasicTxInfo.erase(it);
    Operators.erase(txId);
    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, txId);
    return true;
}

bool TTxController::CancelTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto it = BasicTxInfo.find(txId);
    if (it == BasicTxInfo.end()) {
        return true;
    }
    if (it->second.PlanStep != 0) {
        // Cannot cancel planned transaction
        return false;
    }

    auto opIt = Operators.find(txId);
    Y_ABORT_UNLESS(opIt != Operators.end());
    opIt->second->Abort(Owner, txc);

    if (it->second.MaxStep != Max<ui64>()) {
        DeadlineQueue.erase(TPlanQueueItem(it->second.MaxStep, txId));
    }
    BasicTxInfo.erase(it);
    Operators.erase(txId);
    NIceDb::TNiceDb db(txc.DB);
    Schema::EraseTxInfo(db, txId);
    return true;
}

std::optional<TTxController::TTxInfo> TTxController::StartPlannedTx() {
    if (!PlanQueue.empty()) {
        auto node = PlanQueue.extract(PlanQueue.begin());
        auto& item = node.value();
        TPlanQueueItem tx(item.Step, item.TxId);
        auto it = BasicTxInfo.find(item.TxId);
        Y_ABORT_UNLESS(it != BasicTxInfo.end());
        RunningQueue.emplace(std::move(item));
        return it->second;
    }
    return std::nullopt;
}

void TTxController::FinishPlannedTx(const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    BasicTxInfo.erase(txId);
    Operators.erase(txId);
    Schema::EraseTxInfo(db, txId);
}

void TTxController::CompleteRunningTx(const TPlanQueueItem& txItem) {
    RunningQueue.erase(txItem);
}

std::optional<TTxController::TPlanQueueItem> TTxController::GetPlannedTx() const {
    if (PlanQueue.empty()) {
        return std::nullopt;
    }
    return *PlanQueue.begin();
}

std::optional<TTxController::TTxInfo> TTxController::GetTxInfo(const ui64 txId) const {
    auto txPtr = BasicTxInfo.FindPtr(txId);
    if (txPtr) {
        return *txPtr;
    }
    return std::nullopt;
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
        while (!DeadlineQueue.empty()) {
            auto it = DeadlineQueue.begin();
            if (outdatedStep < it->Step) {
                // This transaction has a chance to be planned
                break;
            }
            ui64 txId = it->TxId;
            LOG_S_DEBUG(TStringBuilder() << "Removing outdated txId " << txId << " max step " << it->Step
                << " outdated step ");
            AbortTx(txId, txc);
            ++removedCount;
        }
    }
    return removedCount;
}

TTxController::EPlanResult TTxController::PlanTx(const ui64 planStep, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc) {
    auto it = BasicTxInfo.find(txId);
    if (it == BasicTxInfo.end()) {
        return EPlanResult::Skipped;
    }
    if (it->second.PlanStep == 0) {
        it->second.PlanStep = planStep;
        NIceDb::TNiceDb db(txc.DB);
        Schema::UpdateTxInfoPlanStep(db, txId, planStep);
        PlanQueue.emplace(planStep, txId);
        if (it->second.MaxStep != Max<ui64>()) {
            DeadlineQueue.erase(TPlanQueueItem(it->second.MaxStep, txId));
        }
        return EPlanResult::Planned;
    }
    return EPlanResult::AlreadyPlanned;
}

void TTxController::OnTabletInit() {
    for (auto&& txOperator : Operators) {
        txOperator.second->OnTabletInit(Owner);
    }
}

}

template <>
void Out<NKikimrTxColumnShard::ETransactionKind>(IOutputStream& out, TTypeTraits<NKikimrTxColumnShard::ETransactionKind>::TFuncParam txKind) {
    out << (ui64) txKind;
}
