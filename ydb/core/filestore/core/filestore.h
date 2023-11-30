#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/filestore_config.pb.h>

namespace NKikimr {

struct TEvFileStore {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FILESTORE),

        EvUpdateConfig = EvBegin + 1,
        EvUpdateConfigResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_FILESTORE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FILESTORE)");

    struct TEvUpdateConfig : TEventPB<TEvUpdateConfig,
            NKikimrFileStore::TUpdateConfig, EvUpdateConfig> {
        TEvUpdateConfig() {}
    };

    struct TEvUpdateConfigResponse : TEventPB<TEvUpdateConfigResponse,
            NKikimrFileStore::TUpdateConfigResponse, EvUpdateConfigResponse> {
        TEvUpdateConfigResponse() {}
    };
};

}
