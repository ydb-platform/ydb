#include "evolution.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TStandaloneSchemaEvolution::DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) {
    if (!proto.HasStandaloneSchema()) {
        return TConclusionStatus::Fail("no appropriate object for deserialize");
    }
    if (!proto.GetStandaloneSchema().HasAlterToShard()) {
        return TConclusionStatus::Fail("haven't alter to shard info");
    }
    AlterToShard = proto.GetStandaloneSchema().GetAlterToShard();
    return TConclusionStatus::Success();
}

void TStandaloneSchemaEvolution::DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const {
    *proto.MutableStandaloneSchema()->MutableAlterToShard() = AlterToShard;
}

}