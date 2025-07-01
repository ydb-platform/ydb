#pragma once

#include "defs.h"
#include "execution_unit_ctors.h"
#include "datashard_active_transaction.h"
#include "datashard_impl.h"
#include "incr_restore_scan.h"
#include "change_exchange_helpers.h"
#include "change_exchange_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/datashard/change_sender_table_base.h>

namespace NKikimr {
namespace NDataShard {

// DataShard-to-DataShard streaming execution unit
// Follows the same pattern as TCreateIncrementalRestoreSrcUnit but for general streaming
class TCreateDataShardStreamingUnit : public TExecutionUnit {
public:
    bool IsRelevant(TActiveTransaction* tx) const override {
        return tx->GetSchemeTx().HasCreateDataShardStreaming();
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(TOperation::TPtr op, const TActorContext& ctx) override;

private:
    bool IsWaiting(TOperation::TPtr op) const {
        return op->IsWaitingForScan() || op->IsWaitingForRestart();
    }

    void SetWaiting(TOperation::TPtr op) {
        op->SetWaitingForScanFlag();
    }

    void ResetWaiting(TOperation::TPtr op) {
        op->ResetWaitingForScanFlag();
        op->ResetWaitingForRestartFlag();
    }

    void Abort(TOperation::TPtr op, const TActorContext& ctx, const TString& error);
    
    // Create change sender that streams to another DataShard
    THolder<NTable::IScan> CreateDataShardStreamingScan(
        const ::NKikimrSchemeOp::TCreateDataShardStreaming& streaming,
        ui64 txId);

public:
    TCreateDataShardStreamingUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::CreateDataShardStreaming, false, dataShard, pipeline)
    {}

    ~TCreateDataShardStreamingUnit() override = default;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CREATE_DATASHARD_STREAMING_UNIT;
    }
};

} // namespace NDataShard
} // namespace NKikimr
