#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>
#include <ydb/library/actors/core/actorid.h>


namespace NYdb::NBS::NBlockStore {

    struct TEvService {

        //
        // Events declaration
        //

        enum EEvents
        {
            EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2),

            EvReadBlocksRequest,
            EvReadBlocksResponse,

            EvWriteBlocksRequest,
            EvWriteBlocksResponse,

            EvGetLoadActorAdapterActorIdRequest,
            EvGetLoadActorAdapterActorIdResponse,
        };

        BLOCKSTORE_DECLARE_PROTO_EVENTS(WriteBlocks)
        BLOCKSTORE_DECLARE_PROTO_EVENTS(ReadBlocks)

        struct TEvGetLoadActorAdapterActorIdRequest
            : public NActors::TEventLocal<TEvGetLoadActorAdapterActorIdRequest, EvGetLoadActorAdapterActorIdRequest>
        {};

        struct TEvGetLoadActorAdapterActorIdResponse
            : public NActors::TEventLocal<TEvGetLoadActorAdapterActorIdResponse, EvGetLoadActorAdapterActorIdResponse>
        {
            TString ActorId;
        };
    };

} // namespace NYdb::NBS::NBlockStore
