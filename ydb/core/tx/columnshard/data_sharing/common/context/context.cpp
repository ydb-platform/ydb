#include "context.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimrColumnShardDataSharingProto::TTransferContext TTransferContext::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TTransferContext result;
    result.SetDestinationTabletId((ui64)DestinationTabletId);
    if (TxId) {
        result.SetTxId(*TxId);
    }
    for (auto&& i : SourceTabletIds) {
        result.AddSourceTabletIds((ui64)i);
    }
    GetSnapshotBarrierVerified().SerializeToProto(*result.MutableSnapshotBarrier());
    result.SetMoving(Moving);
    return result;
}

NKikimr::TConclusionStatus TTransferContext::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TTransferContext& proto) {
    DestinationTabletId = (TTabletId)proto.GetDestinationTabletId();
    if (proto.HasTxId()) {
        TxId = proto.GetTxId();
        if (!*TxId) {
            return TConclusionStatus::Fail("TxId is incorrect");
        }
    }
    if (!(ui64)DestinationTabletId) {
        return TConclusionStatus::Fail("incorrect DestinationTabletId in proto");
    }
    for (auto&& i : proto.GetSourceTabletIds()) {
        AFL_VERIFY(SourceTabletIds.emplace((TTabletId)i).second);
    }
    Moving = proto.GetMoving();
    {
        if (proto.HasSnapshotBarrier()) {
            TSnapshot snapshot = TSnapshot::Zero();
            auto snapshotParse = snapshot.DeserializeFromProto(proto.GetSnapshotBarrier());
            if (!snapshotParse) {
                return snapshotParse;
            }
            if (!snapshot.Valid()) {
                return TConclusionStatus::Fail("SnapshotBarrier must be valid in proto.");
            }
            SnapshotBarrier = snapshot;
        }
    }
    return TConclusionStatus::Success();
}

bool TTransferContext::IsEqualTo(const TTransferContext& context) const {
    return
        DestinationTabletId == context.DestinationTabletId &&
        SourceTabletIds == context.SourceTabletIds &&
        Moving == context.Moving &&
        SnapshotBarrier == context.SnapshotBarrier;
}

TString TTransferContext::DebugString() const {
    return TStringBuilder() << "{from=" << (ui64)DestinationTabletId << ";moving=" << Moving << ";snapshot=" << SnapshotBarrier.DebugString() << "}";
}

TTransferContext::TTransferContext(const TTabletId destination, const THashSet<TTabletId>& sources, const TSnapshot& snapshotBarrier, const bool moving, const std::optional<ui64> txId)
    : DestinationTabletId(destination)
    , SourceTabletIds(sources)
    , Moving(moving)
    , SnapshotBarrier(snapshotBarrier)
    , TxId(txId)
{
    AFL_VERIFY(!TxId || *TxId);
    AFL_VERIFY(!sources.contains(destination));
}

const NKikimr::NOlap::TSnapshot& TTransferContext::GetSnapshotBarrierVerified() const {
    AFL_VERIFY(SnapshotBarrier.Valid());
    return SnapshotBarrier;
}

}