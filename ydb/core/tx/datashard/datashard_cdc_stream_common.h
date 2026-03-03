#pragma once

#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"

namespace NKikimr {
namespace NDataShard {

class TCdcStreamUnitBase : public TExecutionUnit {
protected:
    TVector<THolder<TEvChangeExchange::TEvRemoveSender>> RemoveSenders;
    THolder<TEvChangeExchange::TEvAddSender> AddSender;

public:
    TCdcStreamUnitBase(EExecutionUnitKind kind, bool createCdcStream, TDataShard& self, TPipeline& pipeline);

    void DropCdcStream(
        TTransactionContext& txc,
        const TPathId& tablePathId,
        const TPathId& streamPathId,
        const TUserTable& tableInfo);

    void AddCdcStream(
        TTransactionContext& txc,
        const TPathId& tablePathId,
        const TPathId& streamPathId,
        const NKikimrSchemeOp::TCdcStreamDescription& streamDesc,
        const TString& snapshotName,
        ui64 step,
        ui64 txId);

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override;
};

} // namespace NDataShard
} // namespace NKikimr