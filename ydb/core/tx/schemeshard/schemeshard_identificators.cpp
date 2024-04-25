#include "schemeshard_identificators.h"

namespace NKikimr::NSchemeShard {

NKikimrSchemeOp::TShardIdx AsProto(const NKikimr::NSchemeShard::TShardIdx& shardIdx) {
    NKikimrSchemeOp::TShardIdx proto;
    proto.SetOwnerId(ui64(shardIdx.GetOwnerId()));
    proto.SetLocalId(ui64(shardIdx.GetLocalId()));
    return proto;
}

TShardIdx FromProto(const NKikimrSchemeOp::TShardIdx& shardIdx) {
    return TShardIdx(TOwnerId(shardIdx.GetOwnerId()), TLocalShardIdx(shardIdx.GetLocalId()));
}

NKikimrSchemeOp::TShardIdx TShardIdx::SerializeToProto() const {
    return AsProto(*this);
}

TConclusion<TShardIdx> TShardIdx::BuildFromProto(const NKikimrSchemeOp::TShardIdx& proto) {
    return FromProto(proto);
}
}