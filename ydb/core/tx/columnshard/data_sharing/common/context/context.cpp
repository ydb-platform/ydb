#include "context.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimrColumnShardDataSharingProto::TTransferContext TTransferContext::SerializeToProto() const {
    NKikimrColumnShardDataSharingProto::TTransferContext result;
    result.SetDestinationTabletId((ui64)DestinationTabletId);
    for (auto&& i : SourceTabletIds) {
        result.AddSourceTabletIds((ui64)i);
    }
    result.SetMoving(Moving);
    return result;
}

NKikimr::TConclusionStatus TTransferContext::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TTransferContext& proto) {
    DestinationTabletId = (TTabletId)proto.GetDestinationTabletId();
    if (!(ui64)DestinationTabletId) {
        return TConclusionStatus::Fail("incorrect DestinationTabletId in proto");
    }
    for (auto&& i : proto.GetSourceTabletIds()) {
        AFL_VERIFY(SourceTabletIds.emplace((TTabletId)i).second);
    }
    Moving = proto.GetMoving();
    return TConclusionStatus::Success();
}

}