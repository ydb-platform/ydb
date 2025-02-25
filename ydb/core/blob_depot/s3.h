#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TS3Manager {
        class TUploaderActor;
        struct TEvUploadResult;

        class TDeleterActor;
        class TTxDeleteTrashS3;
        struct TEvDeleteResult;

        TBlobDepot* const Self;
        NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
        TActorId WrapperId;
        TActorId UploaderId;
        TString BasePath;

        bool SyncMode = false;
        bool AsyncMode = false;

        THashSet<TActorId> ActiveUploaders;

        ui64 NextKeyId = 1;

        static constexpr ui32 MaxDeletesInFlight = 3;
        static constexpr size_t MaxObjectsToDeleteAtOnce = 1;

        std::deque<TS3Locator> DeleteQueue;
        THashSet<TActorId> ActiveDeleters;
        ui32 NumDeleteTxInFlight = 0;
        ui64 TotalS3TrashObjects = 0;
        ui64 TotalS3TrashSize = 0;

    public:
        TS3Manager(TBlobDepot *self);
        ~TS3Manager();

        void Init(const NKikimrBlobDepot::TS3BackendSettings *settings);
        void TerminateAllUploaders();

        void Handle(TAutoPtr<IEventHandle> ev);

        void OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain);

        TS3Locator AllocateS3Locator(ui32 len);

        const TActorId& GetWrapperId() const { return WrapperId; }

        void AddTrashToCollect(TS3Locator locator);

        ui64 GetTotalS3TrashObjects() const { return TotalS3TrashObjects; }
        ui64 GetTotalS3TrashSize() const { return TotalS3TrashSize; }

    private:
        void RunDeletersIfNeeded();
    };

} // NKikimr::NBlobDepot
