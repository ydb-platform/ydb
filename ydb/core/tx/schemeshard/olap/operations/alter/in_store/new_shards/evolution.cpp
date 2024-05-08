#include "evolution.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TString TInStoreNewShardsEvolution::DoGetShardTxBody(const TPathId& pathId, const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const {
    NKikimrTxColumnShard::TSchemaTxBody result;

    result.MutableSeqNo()->SetGeneration(seqNo.Generation);
    result.MutableSeqNo()->SetRound(seqNo.Round);

    auto& create = *result.MutableEnsureTables()->AddTables();
    create = CreateToShard;
    create.SetPathId(pathId.LocalPathId);
    return result.SerializeAsString();
}

void TInStoreNewShardsEvolution::DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const {
    auto& alter = *proto.MutableInStoreNewShards();
    *alter.MutableCreateToShard() = CreateToShard;
    AFL_VERIFY(ShardIds.size());
}

NKikimr::TConclusionStatus TInStoreNewShardsEvolution::DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) {
    if (!proto.HasInStoreNewShards()) {
        return TConclusionStatus::Fail("no appropriate object for deserialize");
    }
    CreateToShard = proto.GetInStoreNewShards().GetCreateToShard();
    for (auto&& i : proto.GetShardIds()) {
        ShardIds.emplace(i);
    }
    if (!ShardIds.size()) {
        return TConclusionStatus::Fail("incorrect ShardIds");
    }
    return TConclusionStatus::Success();
}

}