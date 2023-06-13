#pragma once

#include <ydb/core/base/events.h>

#include <library/cpp/grpc/client/grpc_client_low.h>

namespace NKikimr::NFolderService {

struct TEvFolderService {
    enum EEv {
        // requests
        EvGetCloudByFolderRequest = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER),

        // replies
        EvGetCloudByFolderResponse = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER) + 512,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER)");

    struct TEvGetCloudByFolderRequest : TEventLocal<TEvGetCloudByFolderRequest, EvGetCloudByFolderRequest> {
        TString FolderId;
        TString Token;
        TString RequestId;
    };

    struct TEvGetCloudByFolderResponse : TEventLocal<TEvGetCloudByFolderResponse, EvGetCloudByFolderResponse> {
        TString FolderId;
        TString CloudId;
        NGrpc::TGrpcStatus Status;
    };
};
} // namespace NKikimr::NFolderService
