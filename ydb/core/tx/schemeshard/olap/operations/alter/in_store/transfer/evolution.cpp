#include "evolution.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/control.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TString TInStoreShardsTransferEvolution::DoGetShardTxBody(const TPathId& pathId, const ui64 tabletId, const TMessageSeqNo& /*seqNo*/) const {
    AFL_VERIFY(ToShardId == tabletId);
    NKikimrColumnShardDataSharingProto::TEvProposeFromInitiator alter;
    auto& session = *alter.MutableSession();
    session.SetSessionId(GetSessionId());
    {
        auto& remap = *session.AddPathIds();
        remap.SetSourcePathId(pathId.LocalPathId);
        remap.SetDestPathId(pathId.LocalPathId);
    }
    session.MutableInitiatorController()->SetClassName("SCHEMESHARD");
    {
        auto& ctx = *session.MutableTransferContext();
        ctx.SetDestinationTabletId(ToShardId);
        for (auto&& tabletId : FromShardIds) {
            ctx.AddSourceTabletIds(tabletId);
        }
    }
    return alter.SerializeAsString();
}

void TInStoreShardsTransferEvolution::DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const {
    auto& alter = *proto.MutableInStoreShardsTransfer();
    alter.SetToShardId(ToShardId);
    for (auto&& i : FromShardIds) {
        alter.AddFromShardIds(i);
    }
    AFL_VERIFY(ToShardId);
    AFL_VERIFY(FromShardIds.size());
}

NKikimr::TConclusionStatus TInStoreShardsTransferEvolution::DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) {
    if (!proto.HasInStoreShardsTransfer()) {
        return TConclusionStatus::Fail("no appropriate object for deserialize");
    }
    ToShardId = proto.GetInStoreShardsTransfer().GetToShardId();
    if (!ToShardId) {
        return TConclusionStatus::Fail("incorrect ToShardId");
    }
    for (auto&& i : proto.GetInStoreShardsTransfer().GetFromShardIds()) {
        AFL_VERIFY(FromShardIds.emplace(i).second);
    }
    if (!FromShardIds.size()) {
        return TConclusionStatus::Fail("incorrect FromShardIds");
    }
    return TConclusionStatus::Success();
}

}