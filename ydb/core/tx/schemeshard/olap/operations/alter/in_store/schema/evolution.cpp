#include "evolution.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

void TInStoreSchemaEvolution::DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const {
    *proto.MutableInStoreSchema()->MutableAlterToShard() = AlterToShard;
}

NKikimr::TConclusionStatus TInStoreSchemaEvolution::DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) {
    if (!proto.HasInStoreSchema()) {
        return TConclusionStatus::Fail("no appropriate object for deserialize");
    }
    if (!proto.GetInStoreSchema().HasAlterToShard()) {
        return TConclusionStatus::Fail("haven't alter to shard info");
    }
    AlterToShard = proto.GetInStoreSchema().GetAlterToShard();
    return TConclusionStatus::Success();
}

TString TInStoreSchemaEvolution::DoGetShardTxBody(const TPathId& pathId, const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const {
    NKikimrTxColumnShard::TSchemaTxBody result;

    result.MutableSeqNo()->SetGeneration(seqNo.Generation);
    result.MutableSeqNo()->SetRound(seqNo.Round);

    *result.MutableAlterTable() = AlterToShard;
    result.MutableAlterTable()->SetPathId(pathId.LocalPathId);

    return result.SerializeAsString();
}

}