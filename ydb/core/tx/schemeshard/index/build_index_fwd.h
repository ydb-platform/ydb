#pragma once

#include <ydb/core/tx/schemeshard/defs.h>

namespace NKikimr::NSchemeShard {

struct TEvIndexBuilder {
    enum EEv {
        EvCreateRequest = EventSpaceBegin(TKikimrEvents::ES_INDEX_BUILD),
        EvCreateResponse,
        EvGetRequest,
        EvGetResponse,
        EvCancelRequest,
        EvCancelResponse,
        EvForgetRequest,
        EvForgetResponse,
        EvListRequest,
        EvListResponse,
        EvUploadSampleKResponse,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_INDEX_BUILD),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_INDEX_BUILD)"
    );

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
    struct TEvUploadSampleKResponse;
};

#define DECLARE_INDEX_BUILDER_EVENT_HANDLE(alias, event) \
    using alias = TAutoPtr<NActors::TEventHandle<TEvIndexBuilder::event>>;
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderCreateRequest__HandlePtr, TEvCreateRequest)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderCreateResponse__HandlePtr, TEvCreateResponse)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderGetRequest__HandlePtr, TEvGetRequest)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderCancelRequest__HandlePtr, TEvCancelRequest)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderCancelResponse__HandlePtr, TEvCancelResponse)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderForgetRequest__HandlePtr, TEvForgetRequest)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderListRequest__HandlePtr, TEvListRequest)
DECLARE_INDEX_BUILDER_EVENT_HANDLE(TEvIndexBuilderUploadSampleKResponse__HandlePtr, TEvUploadSampleKResponse)
#undef DECLARE_INDEX_BUILDER_EVENT_HANDLE

} // namespace NKikimr::NSchemeShard
