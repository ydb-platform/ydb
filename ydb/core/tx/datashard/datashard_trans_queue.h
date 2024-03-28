#pragma once

#include "datashard_active_transaction.h"
#include "operation.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <util/generic/set.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
class TPipeline;

enum class ECleanupStatus {
    None,
    Restart,
    Success,
};

/// Manages datashard's user transactions
class TTransQueue {
public:
    friend class TPipeline;
    friend class TActiveTransaction;
    friend class TWriteOperation;

    TTransQueue(TDataShard * self)
        : Self(self)
    {}

    void Reset() {
        TxsInFly.clear();
        SchemaOps.clear();
        PlannedTxs.clear();
        DeadlineQueue.clear();
        ProposeDelayers.clear();
        PlanWaitingTxCount = 0;
    }

    bool Load(NIceDb::TNiceDb& db);

    const THashMap<ui64, TOperation::TPtr> &GetTxsInFly() const { return TxsInFly; }
    ui64 TxInFly() const { return TxsInFly.size(); }
    void AddTxInFly(TOperation::TPtr op);
    void RemoveTxInFly(ui64 txId, std::vector<std::unique_ptr<IEventHandle>> *cleanupReplies = nullptr);
    TOperation::TPtr FindTxInFly(ui64 txId) const
    {
        auto it = TxsInFly.find(txId);
        if (it != TxsInFly.end())
            return it->second;
        return nullptr;
    }
    bool Has(ui64 txId) const {return TxsInFly.contains(txId); }

    ui64 TxPlanned() const { return PlannedTxs.size(); }
    const TSet<TStepOrder> &GetPlan() const { return PlannedTxs; }
    const TSet<TStepOrder> &GetPlan(EOperationKind kind) const
    {
        auto it = PlannedTxsByKind.find(kind);
        if (it != PlannedTxsByKind.end())
            return it->second;
        return EMPTY_PLAN;
    }

    TSchemaOperation * FindSchemaTx(ui64 txId) { return SchemaOps.FindPtr(txId); }
    const TMap<ui64, TSchemaOperation>& GetSchemaOperations() const { return SchemaOps; }
    bool HasNotAckedSchemaTx() const { return ! SchemaOps.empty(); }

    ui64 TxPlanWaiting() const { return PlanWaitingTxCount; }

    bool HasProposeDelayers() const { return !ProposeDelayers.empty(); }
    bool RemoveProposeDelayer(ui64 txId) { return ProposeDelayers.erase(txId) > 0; }

    // Debug
    TString TxInFlyToString() const;

private: // for pipeline only

    // Propose

    void ProposeTx(NIceDb::TNiceDb& db, TOperation::TPtr op, TActorId source, const TStringBuf& txBody);
    void UpdateTxFlags(NIceDb::TNiceDb& db, ui64 txId, ui64 flags);
    void UpdateTxBody(NIceDb::TNiceDb& db, ui64 txId, const TStringBuf& txBody);
    void ProposeSchemaTx(NIceDb::TNiceDb& db, const TSchemaOperation& op);
    bool CancelPropose(NIceDb::TNiceDb& db, ui64 txId, std::vector<std::unique_ptr<IEventHandle>>& replies);
    ECleanupStatus CleanupOutdated(NIceDb::TNiceDb& db, ui64 outdatedStep, ui32 batchSize, TVector<ui64>& outdatedTxs);
    bool CleanupVolatile(ui64 txId);

    // Plan

    void PlanTx(TOperation::TPtr op,
                ui64 step,
                NIceDb::TNiceDb &db);
    void ForgetPlannedTx(NIceDb::TNiceDb &db, ui64 step, ui64 txId);

    // Execute

    // get first planned tx starting from {step, txId}
    void GetPlannedTxId(ui64& step, ui64& txId) const;
    bool GetNextPlannedTxId(ui64& step, ui64& txId) const;
    bool LoadTxDetails(NIceDb::TNiceDb &db,
                       ui64 txId,
                       TActorId &targets,
                       TString &txBody,
                       TVector<TSysTables::TLocksTable::TLock> &locks,
                       ui64 &artifactFlags);

    // Done

    void RemoveTx(NIceDb::TNiceDb &db,
                  const TOperation &op);
    void RemoveSchemaOperation(NIceDb::TNiceDb& db, ui64 txId);
    void RemoveScanProgress(NIceDb::TNiceDb& db, ui64 txId);

    ui64 NextDeadlineStep() const {
        if (DeadlineQueue.empty())
            return Max<ui64>();
        return DeadlineQueue.begin()->first;
    }

private:
    TDataShard * Self;
    THashMap<ui64, TOperation::TPtr> TxsInFly;
    TSet<TStepOrder> PlannedTxs;
    THashMap<EOperationKind, TSet<TStepOrder>> PlannedTxsByKind;
    TSet<std::pair<ui64, ui64>> DeadlineQueue; // {maxStep, txId}
    TMap<ui64, TSchemaOperation> SchemaOps; // key - txId
    TSet<ui64> ProposeDelayers;
    ui64 PlanWaitingTxCount = 0;

    static const TSet<TStepOrder> EMPTY_PLAN;

    bool ClearTxDetails(NIceDb::TNiceDb& db, ui64 txId);
};

}}
