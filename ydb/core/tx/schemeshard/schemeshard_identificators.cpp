#include "schemeshard_identificators.h"

NKikimrSchemeOp::TShardIdx NKikimr::NSchemeShard::AsProto(const NKikimr::NSchemeShard::TShardIdx &shardIdx) {
    NKikimrSchemeOp::TShardIdx proto;
    proto.SetOwnerId(ui64(shardIdx.GetOwnerId()));
    proto.SetLocalId(ui64(shardIdx.GetLocalId()));
    return proto;
}

NKikimr::NSchemeShard::TShardIdx NKikimr::NSchemeShard::FromProto(const NKikimrSchemeOp::TShardIdx &shardIdx) {
    return TShardIdx(TOwnerId(shardIdx.GetOwnerId()), TLocalShardIdx(shardIdx.GetLocalId()));
}
