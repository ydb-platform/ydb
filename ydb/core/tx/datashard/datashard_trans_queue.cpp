#include "datashard_trans_queue.h"
#include "datashard_active_transaction.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

const TSet<TStepOrder> TTransQueue::EMPTY_PLAN;

void TTransQueue::AddTxInFly(TOperation::TPtr op) {
    const ui64 txId = op->GetTxId();
    Y_ENSURE(!TxsInFly.contains(txId), "Adding duplicate txId " << txId);
    TxsInFly[txId] = op;
    if (Y_LIKELY(!op->GetStep())) {
        ++PlanWaitingTxCount;
        const ui64 maxStep = op->GetMaxStep();
        if (maxStep != Max<ui64>()) {
            DeadlineQueue.emplace(std::make_pair(maxStep, txId));
        }
    }
    Self->SetCounter(COUNTER_TX_IN_FLY, TxsInFly.size());
}

void TTransQueue::RemoveTxInFly(ui64 txId, std::vector<std::unique_ptr<IEventHandle>> *cleanupReplies) {
    auto it = TxsInFly.find(txId);
    if (it != TxsInFly.end()) {
        if (cleanupReplies) {
            Self->GetCleanupReplies(it->second, *cleanupReplies);
        }
        if (!it->second->GetStep()) {
            const ui64 maxStep = it->second->GetMaxStep();
            if (maxStep != Max<ui64>()) {
                DeadlineQueue.erase(std::make_pair(maxStep, txId));
            }
            --PlanWaitingTxCount;
        }
        TxsInFly.erase(it);
        ProposeDelayers.erase(txId);
        Self->SetCounter(COUNTER_TX_IN_FLY, TxsInFly.size());
        Self->GetConflictsCache().UnregisterDistributedWrites(txId);
    }
}

bool TTransQueue::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    // Load must be idempotent
    Y_ENSURE(TxsInFly.empty());
    Y_ENSURE(SchemaOps.empty());
    Y_ENSURE(PlannedTxs.empty());
    Y_ENSURE(DeadlineQueue.empty());
    Y_ENSURE(ProposeDelayers.empty());
    Y_ENSURE(PlanWaitingTxCount == 0);

    TInstant now = AppData()->TimeProvider->Now();

    THashSet<ui64> schemaTxs;
    {
        auto rowset = db.Table<Schema::TxMain>().Range().Select();
        if (!rowset.IsReady())
            return false;
        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::TxMain::TxId>();
            ui64 flags;
            if (rowset.HaveValue<Schema::TxMain::Flags64>()) {
                flags = rowset.GetValue<Schema::TxMain::Flags64>();
            } else {
                flags = rowset.GetValue<Schema::TxMain::Flags>();
            }
            ui64 maxStep = rowset.GetValue<Schema::TxMain::MaxStep>();
            EOperationKind kind = rowset.GetValue<Schema::TxMain::Kind>();
            ui64 received = rowset.GetValueOrDefault<Schema::TxMain::ReceivedAt>(now.GetValue());
            if (kind == EOperationKind::SchemeTx)
                schemaTxs.insert(txId);

            if (flags & TTxFlags::Immediate) {
                // Workaround for KIKIMR-8005
                flags |= TTxFlags::ForceOnline;
            }

            // We have loaded transaction from the database
            flags |= TTxFlags::Stored;

            TBasicOpInfo info(txId, kind, flags, maxStep, TInstant::FromValue(received), Self->NextTieBreakerIndex++);

            TOperation::TPtr op = NEvWrite::TConvertor::MakeOperation(kind, info, Self->TabletID());

            if (rowset.HaveValue<Schema::TxMain::Source>()) {
                op->SetTarget(rowset.GetValue<Schema::TxMain::Source>());
            }
            if (rowset.HaveValue<Schema::TxMain::Cookie>()) {
                op->SetCookie(rowset.GetValue<Schema::TxMain::Cookie>());
            }
            AddTxInFly(std::move(op));

            if (!rowset.Next())
                return false;
        }
    }

    {
        auto rowset = db.Table<Schema::PlanQueue>().Range().Select();
        if (!rowset.IsReady())
            return false;
        while (!rowset.EndOfSet()) {
            ui64 step = rowset.GetValue<Schema::PlanQueue::Step>();
            ui64 txId = rowset.GetValue<Schema::PlanQueue::TxId>();
            PlannedTxs.emplace(TStepOrder(step, txId));

            auto op = FindTxInFly(txId);
            if (op) {
                if (Y_LIKELY(!op->GetStep())) {
                    op->SetStep(step);
                    --PlanWaitingTxCount;
                    if (op->HasFlag(TTxFlags::BlockingImmediateOps) ||
                        op->HasFlag(TTxFlags::BlockingImmediateWrites))
                    {
                        ProposeDelayers.insert(txId);
                    }
                }
                PlannedTxsByKind[op->GetKind()].emplace(TStepOrder(step, txId));
                DeadlineQueue.erase(std::make_pair(op->GetMaxStep(), op->GetTxId()));
            }

            if (!rowset.Next())
                return false;
        }
    }

    // SchemaOperations
    {
        auto rowset = db.Table<Schema::SchemaOperations>().Range().Select();
        if (!rowset.IsReady())
            return false;
        while (!rowset.EndOfSet()) {
            ui64 txId = rowset.GetValue<Schema::SchemaOperations::TxId>();
            TActorId source = rowset.GetValue<Schema::SchemaOperations::Source>();
            ui64 tabletId = rowset.GetValueOrDefault<Schema::SchemaOperations::SourceTablet>(0);
            ui32 opType = rowset.GetValue<Schema::SchemaOperations::Operation>();
            ui64 minStep = rowset.GetValue<Schema::SchemaOperations::MinStep>();
            ui64 maxStep = rowset.GetValue<Schema::SchemaOperations::MaxStep>();
            ui64 planStep = rowset.GetValueOrDefault<Schema::SchemaOperations::PlanStep>(0);
            ui64 readOnly = rowset.GetValueOrDefault<Schema::SchemaOperations::ReadOnly>(false);
            bool success = rowset.GetValueOrDefault<Schema::SchemaOperations::Success>(false);
            TString error = rowset.GetValueOrDefault<Schema::SchemaOperations::Error>(TString());
            ui64 dataSize = rowset.GetValueOrDefault<Schema::SchemaOperations::DataSize>(0);
            ui64 rows = rowset.GetValueOrDefault<Schema::SchemaOperations::Rows>(0);

            if (!tabletId) { // Remove legacy data from DB. New schema operations has tabletId
                db.Table<Schema::SchemaOperations>().Key(txId).Delete();
                continue;
            }

            TSchemaOperation op(txId, TSchemaOperation::EType(opType), source,
                               tabletId, minStep, maxStep, planStep, readOnly,
                               success, error, dataSize, rows);
            auto saved = SchemaOps.insert(std::make_pair(op.TxId, op));
            TSchemaOperation * savedOp = &saved.first->second;
            if (schemaTxs.contains(txId)) { // is not done yet
                Self->Pipeline.SetSchemaOp(savedOp);
            } else {
                savedOp->Done = true;
            }

            if (!rowset.Next())
                return false;
        }
        // Load scan state (only for scheme tx)
        {
            auto rowset = db.Table<Schema::ScanProgress>().Range().Select();
            if (!rowset.IsReady())
                return false;
            while (!rowset.EndOfSet()) {
                ui64 txId = rowset.GetValue<Schema::ScanProgress::TxId>();
                TSchemaOperation* op = FindSchemaTx(txId);
                if (!op) {
                    LOG_WARN_S(TlsActivationContext->AsActorContext(), NKikimrServices::TX_DATASHARD,
                               "Op was not found for persisted scan tx id " << txId
                               << " on tablet "
                               << Self->TabletID());
                    continue;
                }
                op->ScanState.LastKey = rowset.GetValue<Schema::ScanProgress::LastKey>();
                op->ScanState.Bytes = rowset.GetValue<Schema::ScanProgress::LastBytes>();
                ui64 lastStatus = rowset.GetValue<Schema::ScanProgress::LastStatus>();
                op->ScanState.StatusCode = static_cast<Ydb::StatusIds::StatusCode>(lastStatus);
                auto binaryIssues = rowset.GetValue<Schema::ScanProgress::LastIssues>();
                op->ScanState.Issues = DeserializeIssues(binaryIssues);
                if (!rowset.Next())
                    return false;
            }
        }
    }

    return true;
}

void TTransQueue::ProposeSchemaTx(NIceDb::TNiceDb& db, const TSchemaOperation& op) {
    using Schema = TDataShard::Schema;

    // Auto-ack previous schema operation
    if (!SchemaOps.empty()) {
        Y_ENSURE(SchemaOps.begin()->first != op.TxId, "Duplicate Tx " << op.TxId << " wasn't properly handled");
        Y_ENSURE(SchemaOps.size() == 1, "Cannot have multiple un-Ack-ed previous schema operations");
        Y_ENSURE(SchemaOps.begin()->second.Done,
            "Previous Tx " << SchemaOps.begin()->first << " must be in a state where it only waits for Ack");
        RemoveSchemaOperation(db, SchemaOps.begin()->second.TxId);
    }

    auto saved = SchemaOps.insert(std::make_pair(op.TxId, op));
    db.Table<Schema::SchemaOperations>().Key(op.TxId).Update(
        NIceDb::TUpdate<Schema::SchemaOperations::TxId>(op.TxId),
        NIceDb::TUpdate<Schema::SchemaOperations::Operation>(op.Type),
        NIceDb::TUpdate<Schema::SchemaOperations::Source>(op.Source),
        NIceDb::TUpdate<Schema::SchemaOperations::SourceTablet>(op.TabletId),
        NIceDb::TUpdate<Schema::SchemaOperations::MinStep>(op.MinStep),
        NIceDb::TUpdate<Schema::SchemaOperations::MaxStep>(op.MaxStep),
        NIceDb::TUpdate<Schema::SchemaOperations::PlanStep>(op.PlanStep),
        NIceDb::TUpdate<Schema::SchemaOperations::ReadOnly>(op.ReadOnly),
        NIceDb::TUpdate<Schema::SchemaOperations::Success>(op.Success),
        NIceDb::TUpdate<Schema::SchemaOperations::Error>(op.Error),
        NIceDb::TUpdate<Schema::SchemaOperations::DataSize>(op.BytesProcessed),
        NIceDb::TUpdate<Schema::SchemaOperations::Rows>(op.RowsProcessed));

    TSchemaOperation * savedOp = &saved.first->second;
    Y_ENSURE(savedOp->TabletId);
    Self->Pipeline.SetSchemaOp(savedOp);

    db.NoMoreReadsForTx();
}

void TTransQueue::ProposeTx(NIceDb::TNiceDb& db, TOperation::TPtr op, TActorId source, const TStringBuf& txBody) {
    using Schema = TDataShard::Schema;

    const ui64 preserveFlagsMask = TTxFlags::PublicFlagsMask | TTxFlags::PreservedPrivateFlagsMask;

    AddTxInFly(op);

    if (op->HasVolatilePrepareFlag()) {
        // We keep volatile transactions in memory and don't store anything
        return;
    }

    db.Table<Schema::TxMain>().Key(op->GetTxId()).Update(
        NIceDb::TUpdate<Schema::TxMain::Kind>(op->GetKind()),
        NIceDb::TUpdate<Schema::TxMain::Flags>(op->GetFlags() & preserveFlagsMask),
        NIceDb::TUpdate<Schema::TxMain::MaxStep>(op->GetMaxStep()),
        NIceDb::TUpdate<Schema::TxMain::ReceivedAt>(op->GetReceivedAt().GetValue()),
        NIceDb::TUpdate<Schema::TxMain::Flags64>(op->GetFlags() & preserveFlagsMask),
        NIceDb::TUpdate<Schema::TxMain::Source>(source),
        NIceDb::TUpdate<Schema::TxMain::Cookie>(op->GetCookie()));

    db.Table<Schema::TxDetails>().Key(op->GetTxId(), Self->TabletID()).Update(
        NIceDb::TUpdate<Schema::TxDetails::Body>(TString(txBody)),
        NIceDb::TUpdate<Schema::TxDetails::Source>(source));

    // NOTE: we no longer add rows to the DeadlineQueue table
    db.NoMoreReadsForTx();
}

void TTransQueue::UpdateTxFlags(NIceDb::TNiceDb& db, ui64 txId, ui64 flags) {
    using Schema = TDataShard::Schema;

    auto it = TxsInFly.find(txId);
    Y_ENSURE(it != TxsInFly.end());

    if (it->second->HasVolatilePrepareFlag()) {
        // We keep volatile transactions in memory and don't store anything
        return;
    }

    const ui64 preserveFlagsMask = TTxFlags::PublicFlagsMask | TTxFlags::PreservedPrivateFlagsMask;

    db.Table<Schema::TxMain>().Key(txId)
        .Update<Schema::TxMain::Flags>(flags & preserveFlagsMask)
        .Update<Schema::TxMain::Flags64>(flags & preserveFlagsMask);
}

void TTransQueue::UpdateTxBody(NIceDb::TNiceDb& db, ui64 txId, const TStringBuf& txBody) {
    using Schema = TDataShard::Schema;

    auto it = TxsInFly.find(txId);
    Y_ENSURE(it != TxsInFly.end());

    Y_ENSURE(!it->second->HasVolatilePrepareFlag(), "Unexpected UpdateTxBody for a volatile transaction");

    db.Table<Schema::TxDetails>().Key(txId, Self->TabletID())
        .Update<Schema::TxDetails::Body>(TString(txBody));
}

void TTransQueue::RemoveTx(NIceDb::TNiceDb &db,
                           const TOperation &op) {
    using Schema = TDataShard::Schema;

    if (!op.HasVolatilePrepareFlag()) {
        db.Table<Schema::TxMain>().Key(op.GetTxId()).Delete();
        db.Table<Schema::TxDetails>().Key(op.GetTxId(), Self->TabletID()).Delete();
        db.Table<Schema::TxArtifacts>().Key(op.GetTxId()).Delete();

        db.Table<Schema::PlanQueue>().Key(op.GetStep(), op.GetTxId()).Delete();
        db.Table<Schema::DeadlineQueue>().Key(op.GetMaxStep(), op.GetTxId()).Delete();
    }

    DeadlineQueue.erase(std::make_pair(op.GetMaxStep(), op.GetTxId()));
    RemoveTxInFly(op.GetTxId());
    PlannedTxs.erase(op.GetStepOrder());
    PlannedTxsByKind[op.GetKind()].erase(op.GetStepOrder());
    db.NoMoreReadsForTx();
}

void TTransQueue::RemoveSchemaOperation(NIceDb::TNiceDb& db, ui64 txId) {
    using Schema = TDataShard::Schema;
    SchemaOps.erase(txId);
    db.Table<Schema::SchemaOperations>().Key(txId).Delete();
}

void TTransQueue::RemoveScanProgress(NIceDb::TNiceDb& db, ui64 txId) {
    using Schema = TDataShard::Schema;
    db.Table<Schema::ScanProgress>().Key(txId).Delete();
}

/// @arg inOutStep, inOutTxId - last (complete) tx
/// @return inOutStep, inOutTxId - next planned Tx
void TTransQueue::GetPlannedTxId(ui64& step, ui64& txId) const {
    auto it = PlannedTxs.lower_bound(TStepOrder(step, txId));
    if (it != PlannedTxs.end()) {
        step = it->Step;
        txId = it->TxId;
        return;
    }

    step = txId = 0;
}

bool TTransQueue::GetNextPlannedTxId(ui64& step, ui64& txId) const {
    auto it = PlannedTxs.upper_bound(TStepOrder(step, txId));
    if (it != PlannedTxs.end()) {
        step = it->Step;
        txId = it->TxId;
        return true;
    }

    return false;
}

bool TTransQueue::LoadTxDetails(NIceDb::TNiceDb &db,
                                ui64 txId,
                                TActorId &target,
                                TString &txBody,
                                TVector<TSysTables::TLocksTable::TLock> &locks,
                                ui64 &artifactFlags) {
    using Schema = TDataShard::Schema;

    auto it = TxsInFly.find(txId);
    Y_ENSURE(it != TxsInFly.end());

    Y_ENSURE(!it->second->HasVolatilePrepareFlag(), "Unexpected LoadTxDetails for a volatile transaction");

    auto detailsRow = db.Table<Schema::TxDetails>().Key(txId, Self->TabletID()).Select();
    auto artifactsRow = db.Table<Schema::TxArtifacts>().Key(txId).Select();

    if (!detailsRow.IsReady() || !artifactsRow.IsReady())
        return false;

    Y_ENSURE(!detailsRow.EndOfSet(), "cannot find details for tx=" << txId);

    txBody = detailsRow.GetValue<Schema::TxDetails::Body>();
    target = detailsRow.GetValue<Schema::TxDetails::Source>();

    artifactFlags = 0;
    if (!artifactsRow.EndOfSet()) {
        artifactFlags = artifactsRow.GetValue<Schema::TxArtifacts::Flags>();
        locks.clear();

        // Deserialize persisted locks from database
        // Unfortunately there was a bad version briefly deployed to clusters
        // that had an additional field in the locks structure, so we have to
        // guess an element data size. Luckily there's always either 0 or 1
        // locks in the column, so guessing is not really ambiguous.
        TStringBuf data = artifactsRow.GetValueOrDefault<Schema::TxArtifacts::Locks>({});
        if (!data.empty()) {
            size_t elementSize = sizeof(TSysTables::TLocksTable::TPersistentLock);
            if ((data.size() % elementSize) != 0) {
                size_t badElementSize = elementSize + 8;
                Y_ENSURE((data.size() % badElementSize) == 0,
                    "Unexpected Schema::TxArtifacts::Locks column size " << data.size()
                    << " (expected divisible by " << elementSize << " or " << badElementSize << ")");
                elementSize = badElementSize;
            }

            const char* p = data.data();
            size_t count = data.size() / elementSize;
            locks.reserve(count);

            for (size_t i = 0; i < count; ++i) {
                locks.push_back(TSysTables::TLocksTable::TLock::FromPersistent(
                    ReadUnaligned<TSysTables::TLocksTable::TPersistentLock>(p)));
                p += elementSize;
            }
        }
    }

    return true;
}

// Clear TxDetails for all origins
bool TTransQueue::ClearTxDetails(NIceDb::TNiceDb& db, ui64 txId) {
    using Schema = TDataShard::Schema;

    auto txdRowset = db.Table<Schema::TxDetails>().Prefix(txId).Select();
    if (!txdRowset.IsReady())
        return false;
    while (!txdRowset.EndOfSet()) {
        Y_ENSURE(txId == txdRowset.GetValue<Schema::TxDetails::TxId>());
        ui64 origin = txdRowset.GetValue<Schema::TxDetails::Origin>();
        db.Table<Schema::TxDetails>().Key(txId, origin).Delete();
        if (!txdRowset.Next())
            return false;
    }

    db.Table<Schema::TxArtifacts>().Key(txId).Delete();

    return true;
}

bool TTransQueue::CancelPropose(NIceDb::TNiceDb& db, ui64 txId, std::vector<std::unique_ptr<IEventHandle>>& replies) {
    using Schema = TDataShard::Schema;

    auto it = TxsInFly.find(txId);
    if (it == TxsInFly.end())
        return true; // already cleaned up

    ui64 maxStep = it->second->GetMaxStep();

    if (!it->second->HasVolatilePrepareFlag()) {
        if (!ClearTxDetails(db, txId)) {
            return false;
        }

        db.Table<Schema::DeadlineQueue>().Key(maxStep, txId).Delete();
        db.Table<Schema::TxMain>().Key(txId).Delete();
    }

    DeadlineQueue.erase(std::make_pair(maxStep, txId));
    RemoveTxInFly(txId, &replies);
    Self->IncCounter(COUNTER_PREPARE_CANCELLED);
    return true;
}

// Cleanup outdated transactions.
// The argument outdatedStep specifies the maximum step for which we received
// all planned transactions.
// NOTE: DeadlineQueue no longer contains planned transactions.
ECleanupStatus TTransQueue::CleanupOutdated(NIceDb::TNiceDb& db, ui64 outdatedStep, ui32 batchSize,
        TVector<ui64>& outdatedTxs)
{
    using Schema = TDataShard::Schema;

    outdatedTxs.reserve(batchSize);
    TSet<std::pair<ui64, ui64>> erasedDeadlines;

    for (const auto& pr : DeadlineQueue) {
        ui64 maxStep = pr.first;
        ui64 txId = pr.second;
        if (maxStep > outdatedStep)
            break;

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Cleaning up tx " << txId << " with maxStep " << maxStep << " at outdatedStep " << outdatedStep);

        auto it = TxsInFly.find(txId);
        if (it != TxsInFly.end() && !it->second->HasVolatilePrepareFlag()) {
            if (!ClearTxDetails(db, txId)) {
                return ECleanupStatus::Restart;
            }

            db.Table<Schema::DeadlineQueue>().Key(maxStep, txId).Delete();
            db.Table<Schema::TxMain>().Key(txId).Delete();
        }

        erasedDeadlines.insert(pr);
        outdatedTxs.push_back(txId);
        if (outdatedTxs.size() == batchSize)
            break;
    }

    if (outdatedTxs.empty())
        return ECleanupStatus::None;

    // Removing outdated txIds from in-memory set, so transactions must not restart after this
    db.NoMoreReadsForTx();
    for (const auto& pr : erasedDeadlines) {
        DeadlineQueue.erase(pr);
    }

    // We don't call RemoveTxInFly to give caller a chance to work with them
    // Caller is expected to call RemoveTxInFly on all outdated txs

    Self->IncCounter(COUNTER_TX_PROGRESS_OUTDATED, outdatedTxs.size());
    return ECleanupStatus::Success;
}

bool TTransQueue::CleanupVolatile(ui64 txId) {
    auto it = TxsInFly.find(txId);
    if (it != TxsInFly.end() && it->second->HasVolatilePrepareFlag() && !it->second->GetStep()) {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD,
                "Cleaning up volatile tx " << txId << " ahead of time");

        // We don't call RemoveTxInFly to give caller a chance to work with the operation
        // Caller must call RemoveTxInFly on the transaction

        Self->IncCounter(COUNTER_TX_PROGRESS_OUTDATED, 1);
        return true;
    }

    return false;
}

void TTransQueue::PlanTx(TOperation::TPtr op,
                         ui64 step,
                         NIceDb::TNiceDb &db)
{
    Y_DEBUG_ABORT_UNLESS(TxsInFly.contains(op->GetTxId()) && TxsInFly.at(op->GetTxId()) == op);

    if (Y_LIKELY(!op->GetStep())) {
        op->SetStep(step);
        --PlanWaitingTxCount;
    } else {
        Y_ENSURE(op->GetStep() == step,
                "Tx " << op->GetTxId() << " must not change step from " << op->GetStep() << " to " << step);
    }

    using Schema = TDataShard::Schema;
    if (!op->HasVolatilePrepareFlag()) {
        db.Table<Schema::PlanQueue>().Key(step, op->GetTxId()).Update();
    }
    PlannedTxs.emplace(op->GetStepOrder());
    PlannedTxsByKind[op->GetKind()].emplace(op->GetStepOrder());
    DeadlineQueue.erase(std::make_pair(op->GetMaxStep(), op->GetTxId()));
}

void TTransQueue::ForgetPlannedTx(NIceDb::TNiceDb &db, ui64 step, ui64 txId)
{
    using Schema = TDataShard::Schema;
    db.Table<Schema::PlanQueue>().Key(step, txId).Delete();
    PlannedTxs.erase(TStepOrder(step, txId));
    for (auto& kv : PlannedTxsByKind) {
        // We don't know the exact tx kind, so have to clear all of them
        kv.second.erase(TStepOrder(step, txId));
    }
    Self->IncCounter(COUNTER_TX_PROGRESS_DUPLICATE);
}

TString TTransQueue::TxInFlyToString() const
{
    TStringStream ss;
    for (auto &pr : TxsInFly)
        ss << "{" << pr.first << ": " << pr.second->GetKind() << "} ";
    return ss.Str();
}

}}
