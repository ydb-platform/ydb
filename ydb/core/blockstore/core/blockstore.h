#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blockstore_config.pb.h>

namespace NKikimr {

struct TEvBlockStore {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_BLOCKSTORE) + 1011,

        EvUpdateVolumeConfig = EvBegin + 13,
        EvUpdateVolumeConfigResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOCKSTORE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOCKSTORE)");

    struct TEvUpdateVolumeConfig : TEventPB<TEvUpdateVolumeConfig,
            NKikimrBlockStore::TUpdateVolumeConfig, EvUpdateVolumeConfig> {
        TEvUpdateVolumeConfig() {}
    };

    struct TEvUpdateVolumeConfigResponse : TEventPB<TEvUpdateVolumeConfigResponse,
            NKikimrBlockStore::TUpdateVolumeConfigResponse, EvUpdateVolumeConfigResponse> {
        TEvUpdateVolumeConfigResponse() {}
    };
};

}
