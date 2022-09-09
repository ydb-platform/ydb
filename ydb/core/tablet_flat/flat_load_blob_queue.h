#pragma once

#include "flat_load_blob.h"

#include <ydb/core/base/blobstorage.h>
#include <util/generic/deque.h>
#include <util/generic/hash_multi_map.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TLoadBlobQueueConfig {
        ui64 TabletID = 0;
        ui32 Generation = 0;
        bool Follower = false;
        ::NMonitoring::TDynamicCounters::TCounterPtr NoDataCounter;
        ui64 MaxBytesInFly = 12 * 1024 * 1024;
        NKikimrBlobStorage::EGetHandleClass ReadPrio = NKikimrBlobStorage::FastRead;
    };

    class TLoadBlobQueue {
    public:
        TLoadBlobQueue();

        void Clear();

        void Enqueue(const TLogoBlobID& id, ui32 group, ILoadBlob* load, uintptr_t cookie = 0);

        bool SendRequests(const TActorId& sender);

        bool ProcessResult(TEvBlobStorage::TEvGetResult* msg);

    private:
        struct TPendingItem {
            TLogoBlobID ID;
            ui32 Group;
            ILoadBlob* Load;
            uintptr_t Cookie;
        };

        struct TActiveItem {
            ILoadBlob* Load;
            uintptr_t Cookie;
        };

    public:
        TLoadBlobQueueConfig Config;

    private:
        ui64 ActiveBytesInFly = 0;

        using TActive = THashMultiMap<TLogoBlobID, TActiveItem>;

        TDeque<TPendingItem> Queue;
        TActive Active;
    };

}
}
