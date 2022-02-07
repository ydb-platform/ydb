#pragma once

#include "defs.h"

namespace NKikimr::NTestShard {

    struct TEvTestShard {
        enum {
            EvControlRequest = EventSpaceBegin(TKikimrEvents::ES_TEST_SHARD),
            EvControlResponse,
            EvStateServerConnect,
            EvStateServerDisconnect,
            EvStateServerStatus,
            EvStateServerRequest,
            EvStateServerWriteResult,
            EvStateServerReadResult,
        };
    };

    struct TEvControlRequest : TEventPB<TEvControlRequest, NKikimrClient::TTestShardControlRequest, TEvTestShard::EvControlRequest> {
    };

    struct TEvControlResponse : TEventPB<TEvControlResponse, NKikimrClient::TTestShardControlResponse, TEvTestShard::EvControlResponse> {
    };

} // NKikimr::NTestShard
