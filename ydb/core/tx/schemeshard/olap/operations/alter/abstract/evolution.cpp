#include "evolution.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/protos/evolution.pb.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus ISSEntityEvolution::DeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) {
    AFL_VERIFY(ShardIds.empty());
    for (auto&& i : proto.GetShardIds()) {
        AFL_VERIFY(ShardIds.emplace(i).second);
    }
    if (ShardIds.empty()) {
        return TConclusionStatus::Fail("no shards");
    }
    return DoDeserializeFromProto(proto);
}

void ISSEntityEvolution::SerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const {
    AFL_VERIFY(ShardIds.size());
    for (auto&& i : ShardIds) {
        proto.AddShardIds(i);
    }
    return DoSerializeToProto(proto);
}

}