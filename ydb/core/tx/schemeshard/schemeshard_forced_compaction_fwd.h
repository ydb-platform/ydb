#pragma once

#include "defs.h"

namespace NKikimrForcedCompaction {
class TForcedCompaction;
}

namespace NKikimr::NSchemeShard {

struct TEvForcedCompaction {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_FORCED_COMPACTION),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,
        EvCancelRequest,
        EvCancelResponse,
        EvForgetRequest,
        EvForgetResponse,
        EvListRequest,
        EvListResponse,

        EvEnd
    };

    struct TEvCreateRequest;
    struct TEvCreateResponse;
    struct TEvGetRequest;
    struct TEvGetResponse;
    struct TEvCancelRequest;
    struct TEvCancelResponse;
    struct TEvForgetRequest;
    struct TEvForgetResponse;
    struct TEvListRequest;
    struct TEvListResponse;
};

using TEvForcedCompactionCreateRequest__HandlePtr =
    TAutoPtr<NActors::TEventHandle<TEvForcedCompaction::TEvCreateRequest>>;
using TEvForcedCompactionGetRequest__HandlePtr =
    TAutoPtr<NActors::TEventHandle<TEvForcedCompaction::TEvGetRequest>>;
using TEvForcedCompactionCancelRequest__HandlePtr =
    TAutoPtr<NActors::TEventHandle<TEvForcedCompaction::TEvCancelRequest>>;
using TEvForcedCompactionForgetRequest__HandlePtr =
    TAutoPtr<NActors::TEventHandle<TEvForcedCompaction::TEvForgetRequest>>;
using TEvForcedCompactionListRequest__HandlePtr =
    TAutoPtr<NActors::TEventHandle<TEvForcedCompaction::TEvListRequest>>;

} // namespace NKikimr::NSchemeShard
