#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

#include <util/system/mutex.h>

namespace NKikimr {

struct TEvVMockCtlRequest : public TEventLocal<TEvVMockCtlRequest, TEvBlobStorage::EvVMockCtlRequest> {
    enum class EAction {
        DeleteBlobs,
        SetErrorMode,
        WipeOutBlobs,
    };

    EAction Action;
    TLogoBlobID First; // starting blob id
    TLogoBlobID Last; // ending blob id, also included
    bool ErrorMode = false;
    bool LostMode = false;

    static TEvVMockCtlRequest *CreateDeleteBlobsRequest(const TLogoBlobID& first, const TLogoBlobID& last) {
        TEvVMockCtlRequest *request = new TEvVMockCtlRequest(EAction::DeleteBlobs);
        request->First = first;
        request->Last = last;
        return request;
    }

    static TEvVMockCtlRequest *CreateErrorModeRequest(bool errorMode, bool lostMode = false) {
        TEvVMockCtlRequest *request = new TEvVMockCtlRequest(EAction::SetErrorMode);
        request->ErrorMode = errorMode;
        request->LostMode = lostMode;
        return request;
    }

    static TEvVMockCtlRequest *CreateWipeOutBlobsRequest(const TLogoBlobID& first, const TLogoBlobID& last) {
        TEvVMockCtlRequest *request = new TEvVMockCtlRequest(EAction::WipeOutBlobs);
        request->First = first;
        request->Last = last;
        return request;
    }

private:
    TEvVMockCtlRequest(EAction action)
        : Action(action)
    {}
};

struct TEvVMockCtlResponse : public TEventLocal<TEvVMockCtlResponse, TEvBlobStorage::EvVMockCtlResponse> {
};

struct TVDiskMockSharedState : public TThrRefBase {
    const TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TMutex Mutex;
    THashMap<TLogoBlobID, TIngress> BlobToIngressMap;

    TVDiskMockSharedState(TIntrusivePtr<TBlobStorageGroupInfo> groupInfo)
        : GroupInfo(std::move(groupInfo))
    {}

    TVDiskMockSharedState(const TVDiskMockSharedState& other) = delete;
};

extern NActors::IActor *CreateVDiskMockActor(const TVDiskID& vdiskId,
    TIntrusivePtr<TVDiskMockSharedState> shared,
    std::shared_ptr<TBlobStorageGroupInfo::TTopology> top);

} // NKikimr
