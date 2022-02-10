#pragma once 
 
#include <ydb/library/folder_service/proto/folder_service.pb.h>
#include <ydb/core/base/events.h>
 
#include <library/cpp/grpc/client/grpc_client_low.h> 
 
namespace NKikimr::NFolderService { 
 
struct TEvFolderService { 
    enum EEv { 
        // requests 
        EvGetFolderRequest = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER), 
 
        // replies 
        EvGetFolderResponse = EventSpaceBegin(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER) + 512, 
 
        EvEnd 
    }; 
 
    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER)"); 
 
    struct TEvGetFolderRequest : TEventLocal<TEvGetFolderRequest, EvGetFolderRequest> { 
        NKikimrProto::NFolderService::GetFolderRequest Request; 
        TString Token; 
        TString RequestId; 
    }; 
 
    struct TEvGetFolderResponse : TEventLocal<TEvGetFolderResponse, EvGetFolderResponse> { 
        NKikimrProto::NFolderService::GetFolderResponse Response; 
        NGrpc::TGrpcStatus Status; 
    }; 
}; 
} // namespace NKikimr::NFolderService 
