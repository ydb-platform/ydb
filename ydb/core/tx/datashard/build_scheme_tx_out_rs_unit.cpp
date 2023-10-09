#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TBuildSchemeTxOutRSUnit : public TExecutionUnit {
public:
    TBuildSchemeTxOutRSUnit(TDataShard &dataShard,
                            TPipeline &pipeline);
    ~TBuildSchemeTxOutRSUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TBuildSchemeTxOutRSUnit::TBuildSchemeTxOutRSUnit(TDataShard &dataShard,
                                                 TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::BuildSchemeTxOutRS, false, dataShard, pipeline)
{
}

TBuildSchemeTxOutRSUnit::~TBuildSchemeTxOutRSUnit()
{
}

bool TBuildSchemeTxOutRSUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TBuildSchemeTxOutRSUnit::Execute(TOperation::TPtr op,
                                                  TTransactionContext &txc,
                                                  const TActorContext &)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasSendSnapshot())
        return EExecutionStatus::Executed;

    Y_ABORT_UNLESS(!op->InputSnapshots().empty(), "Snapshots expected");

    auto &outReadSets = op->OutReadSets();
    ui64 srcTablet = DataShard.TabletID();

    const auto& snapshot = schemeTx.GetSendSnapshot();
    ui64 targetTablet = snapshot.GetSendTo(0).GetShard();
    ui64 tableId = snapshot.GetTableId_Deprecated();
    if (snapshot.HasTableId()) {
        Y_ABORT_UNLESS(DataShard.GetPathOwnerId() == snapshot.GetTableId().GetOwnerId());
        tableId = snapshot.GetTableId().GetTableId();
    }
    Y_ABORT_UNLESS(DataShard.GetUserTables().contains(tableId));
    ui32 localTableId = DataShard.GetUserTables().at(tableId)->LocalTid;

    for (auto &snapshot : op->InputSnapshots()) {
        auto* txSnapshot = dynamic_cast<TTxTableSnapshotContext*>(snapshot.Get());
        Y_ABORT_UNLESS(txSnapshot, "Unexpected input snapshot type");

        TString snapBody = DataShard.BorrowSnapshot(localTableId, *snapshot, { }, { }, targetTablet);
        txc.Env.DropSnapshot(snapshot);

        Y_ABORT_UNLESS(snapBody, "Failed to make full borrow snap. w/o tx restarts");

        TString rsBody;
        bool extended = false;

        const bool mvcc = DataShard.IsMvccEnabled();

        TRowVersion minVersion = mvcc ? TRowVersion(op->GetStep(), op->GetTxId()).Next()
                                      : DataShard.GetSnapshotManager().GetMinWriteVersion();
        TRowVersion completeEdge = mvcc ? DataShard.GetSnapshotManager().GetCompleteEdge()
                                    : TRowVersion::Min();
        TRowVersion incompleteEdge = mvcc ? DataShard.GetSnapshotManager().GetIncompleteEdge()
                                     : TRowVersion::Min();
        TRowVersion immediateWriteEdge = mvcc ? DataShard.GetSnapshotManager().GetImmediateWriteEdge()
                                              : TRowVersion::Min();
        TRowVersion lowWatermark = mvcc ? DataShard.GetSnapshotManager().GetLowWatermark() :
                                   TRowVersion::Min();

        if (minVersion || completeEdge || incompleteEdge || immediateWriteEdge || lowWatermark || txSnapshot->HasOpenTxs())
            extended = true; // Must use an extended format

        if (extended) {
            // New format, wrap in an additional protobuf layer
            NKikimrTxDataShard::TSnapshotTransferReadSet rs;

            // Use lz4 compression so snapshots use less bandwidth
            TString compressedBody = NBlockCodecs::Codec("lz4fast")->Encode(snapBody);

            // TODO: make it possible to send multiple tables
            rs.SetBorrowedSnapshot(compressedBody);

            if (minVersion) {
                rs.SetMinWriteVersionStep(minVersion.Step);
                rs.SetMinWriteVersionTxId(minVersion.TxId);
            }

            if (completeEdge) {
                rs.SetMvccCompleteEdgeStep(completeEdge.Step);
                rs.SetMvccCompleteEdgeTxId(completeEdge.TxId);
            }

            if (incompleteEdge) {
                rs.SetMvccIncompleteEdgeStep(incompleteEdge.Step);
                rs.SetMvccIncompleteEdgeTxId(incompleteEdge.TxId);
            }

            if (immediateWriteEdge) {
                rs.SetMvccImmediateWriteEdgeStep(immediateWriteEdge.Step);
                rs.SetMvccImmediateWriteEdgeTxId(immediateWriteEdge.TxId);
            }

            if (lowWatermark) {
                rs.SetMvccLowWatermarkStep(lowWatermark.Step);
                rs.SetMvccLowWatermarkTxId(lowWatermark.TxId);
            }

            if (txSnapshot->HasOpenTxs()) {
                rs.SetWithOpenTxs(true);
            }

            rsBody.reserve(SnapshotTransferReadSetMagic.size() + rs.ByteSizeLong());
            rsBody.append(SnapshotTransferReadSetMagic);
            Y_PROTOBUF_SUPPRESS_NODISCARD rs.AppendToString(&rsBody);
        } else {
            // Legacy format, no additional protobuf layer
            rsBody = snapBody;
        }

        outReadSets[std::make_pair(srcTablet, targetTablet)] = rsBody;
    }

    op->InputSnapshots().clear();

    return EExecutionStatus::Executed;
}

void TBuildSchemeTxOutRSUnit::Complete(TOperation::TPtr,
                                       const TActorContext &)
{
}

THolder<TExecutionUnit> CreateBuildSchemeTxOutRSUnit(TDataShard &dataShard,
                                                     TPipeline &pipeline)
{
    return MakeHolder<TBuildSchemeTxOutRSUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
