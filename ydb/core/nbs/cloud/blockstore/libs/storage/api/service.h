#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/partition.pb.h>

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/actorid.h>

namespace NYdb::NBS::NBlockStore {

struct TEvService
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2_SERVICE),

        EvReadBlocksRequest,
        EvReadBlocksResponse,

        EvWriteBlocksRequest,
        EvWriteBlocksResponse,

        EvGetLoadActorAdapterActorIdRequest,
        EvGetLoadActorAdapterActorIdResponse,

        EvDeletePartitionRequest,
        EvDeletePartitionResponse,
    };

    BLOCKSTORE_DECLARE_PROTO_EVENTS(WriteBlocks)
    BLOCKSTORE_DECLARE_PROTO_EVENTS(ReadBlocks)
    BLOCKSTORE_DECLARE_PROTO_EVENTS(GetLoadActorAdapterActorId)
    BLOCKSTORE_DECLARE_PROTO_EVENTS(DeletePartition)
};

}   // namespace NYdb::NBS::NBlockStore
