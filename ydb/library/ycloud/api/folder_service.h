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
            EvListFolderRequest = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE),

            // replies
            EvListFolderResponse = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE) + 512,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE)");

        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/identity/proto/resourcemanager/v1/folder.proto
        // https://a.yandex-team.ru/arc/trunk/arcadia/cloud/identity/proto/resourcemanager/v1/transitional/folder_service.proto

        struct TEvListFolderRequest : TEvGrpcProtoRequest<TEvListFolderRequest, EvListFolderRequest, yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersRequest> {};
        struct TEvListFolderResponse : TEvGrpcProtoResponse<TEvListFolderResponse, EvListFolderResponse, yandex::cloud::priv::resourcemanager::v1::transitional::ListFoldersResponse> {};
    };
}
