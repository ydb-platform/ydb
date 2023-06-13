#pragma once
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/public/api/client/yc_private/resourcemanager/folder_service.grpc.pb.h>
#include "events.h"

namespace NCloud {
    using namespace NKikimr;

    struct TEvFolderService {
        enum EEv {
            // requests
            EvResolveFoldersRequest = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE) + 1024,

            // replies
            EvResolveFoldersResponse = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE) + 1024 + 512,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE)");

        struct TEvResolveFoldersRequest : TEvGrpcProtoRequest<TEvResolveFoldersRequest, EvResolveFoldersRequest, yandex::cloud::priv::resourcemanager::v1::ResolveFoldersRequest> {};
        struct TEvResolveFoldersResponse : TEvGrpcProtoResponse<TEvResolveFoldersResponse, EvResolveFoldersResponse, yandex::cloud::priv::resourcemanager::v1::ResolveFoldersResponse> {};
    };
}
