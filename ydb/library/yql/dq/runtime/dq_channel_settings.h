#pragma once

#include "dq_channel_storage.h"

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <util/generic/size_literals.h>

namespace NYql::NDq {

struct TDqChannelSettings {

    // Common settings

    NKikimr::NMiniKQL::TType* RowType = nullptr;
    const NKikimr::NMiniKQL::THolderFactory* HolderFactory = nullptr;
    ui64 ChannelId = 0;
    ui32 SrcStageId = 0;
    ui32 DstStageId = 0;
    TCollectStatsLevel Level = TCollectStatsLevel::None;
    NDqProto::EDataTransportVersion TransportVersion = NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0;
    NKikimr::NMiniKQL::EValuePackerVersion PackerVersion = NKikimr::NMiniKQL::EValuePackerVersion::V0;
    ui64 MaxStoredBytes = 8_MB;

    // Output channels settings (may changed in future)

    ui64 MaxChunkBytes = 2_MB;
    ui64 ChunkSizeLimit = 48_MB;
    IDqChannelStorage::TPtr ChannelStorage;
    TMaybe<ui8> ArrayBufferMinFillPercentage;
    TMaybe<size_t> BufferPageAllocSize;
};

} // namespace NYql::NDq