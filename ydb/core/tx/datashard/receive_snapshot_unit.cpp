#include "datashard_impl.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

namespace NKikimr {
namespace NDataShard {

class TReceiveSnapshotUnit : public TExecutionUnit {
public:
    TReceiveSnapshotUnit(TDataShard &dataShard,
                         TPipeline &pipeline);
    ~TReceiveSnapshotUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TReceiveSnapshotUnit::TReceiveSnapshotUnit(TDataShard &dataShard,
                                           TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::ReceiveSnapshot, false, dataShard, pipeline)
{
}

TReceiveSnapshotUnit::~TReceiveSnapshotUnit()
{
}

bool TReceiveSnapshotUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TReceiveSnapshotUnit::Execute(TOperation::TPtr op,
                                               TTransactionContext &txc,
                                               const TActorContext &)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_VERIFY_S(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasReceiveSnapshot())
        return EExecutionStatus::Executed;

    NIceDb::TNiceDb db(txc.DB);

    Y_VERIFY(schemeTx.HasCreateTable());

    const bool mvcc = DataShard.IsMvccEnabled();

    for (auto &pr : op->InReadSets()) {
        for (auto& rsdata : pr.second) {
            NKikimrTxDataShard::TSnapshotTransferReadSet rs;

            // We currently support a single readset for a single user table
            Y_VERIFY(DataShard.GetUserTables().size() == 1, "Support for more than 1 user table in a datashard is not implemented here");
            ui32 localTableId = DataShard.GetUserTables().begin()->second->LocalTid;

            TString snapBody = rsdata.Body;

            if (rsdata.Body.StartsWith(SnapshotTransferReadSetMagic)) {
                const bool ok = rs.ParseFromArray(
                    rsdata.Body.data() + SnapshotTransferReadSetMagic.size(),
                    rsdata.Body.size() - SnapshotTransferReadSetMagic.size());
                Y_VERIFY(ok, "Failed to parse snapshot transfer readset");

                TString compressedBody = rs.GetBorrowedSnapshot();
                snapBody = NBlockCodecs::Codec("lz4fast")->Decode(compressedBody);

                TRowVersion minVersion(rs.GetMinWriteVersionStep(), rs.GetMinWriteVersionTxId());
                if (DataShard.GetSnapshotManager().GetMinWriteVersion() < minVersion)
                    DataShard.GetSnapshotManager().SetMinWriteVersion(db, minVersion);

                if (mvcc) {
                    TRowVersion completeEdge(rs.GetMvccCompleteEdgeStep(), rs.GetMvccCompleteEdgeTxId());
                    if (DataShard.GetSnapshotManager().GetCompleteEdge() < completeEdge)
                        DataShard.GetSnapshotManager().SetCompleteEdge(db, completeEdge);
                    TRowVersion incompleteEdge(rs.GetMvccIncompleteEdgeStep(), rs.GetMvccIncompleteEdgeTxId());
                    if (DataShard.GetSnapshotManager().GetIncompleteEdge() < incompleteEdge)
                        DataShard.GetSnapshotManager().SetIncompleteEdge(db, incompleteEdge);
                    TRowVersion immediateWriteEdge(rs.GetMvccImmediateWriteEdgeStep(), rs.GetMvccImmediateWriteEdgeTxId());
                    if (DataShard.GetSnapshotManager().GetImmediateWriteEdge() < immediateWriteEdge)
                        DataShard.GetSnapshotManager().SetImmediateWriteEdge(immediateWriteEdge, txc);
                    TRowVersion lowWatermark(rs.GetMvccLowWatermarkStep(), rs.GetMvccLowWatermarkTxId());
                    if (DataShard.GetSnapshotManager().GetLowWatermark() < lowWatermark)
                        DataShard.GetSnapshotManager().SetLowWatermark(db, lowWatermark);
                }
            }

            txc.Env.LoanTable(localTableId, snapBody);
        }
    }

    Y_VERIFY(DataShard.GetSnapshotManager().GetSnapshots().empty(),
        "Found unexpected persistent snapshots at CopyTable destination");

    const auto minVersion = mvcc ? DataShard.GetSnapshotManager().GetLowWatermark()
                                 : DataShard.GetSnapshotManager().GetMinWriteVersion();

    // If MinWriteVersion is not zero, then all versions below it are inaccessible
    if (minVersion) {
        for (const auto& kv : DataShard.GetUserTables()) {
            ui32 localTableId = kv.second->LocalTid;
            txc.DB.RemoveRowVersions(localTableId, TRowVersion::Min(), minVersion);
        }
    }

    return EExecutionStatus::ExecutedNoMoreRestarts;
}

void TReceiveSnapshotUnit::Complete(TOperation::TPtr,
                                    const TActorContext &)
{
}

THolder<TExecutionUnit> CreateReceiveSnapshotUnit(TDataShard &dataShard,
                                                  TPipeline &pipeline)
{
    return MakeHolder<TReceiveSnapshotUnit>(dataShard, pipeline);
}

} // namespace NDataShard
} // namespace NKikimr
