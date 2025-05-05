#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TS3Manager {
        TBlobDepot* const Self;
        TActorId WrapperId;
        TActorId UploaderId;
        TString BasePath;
        TString Bucket;

        bool SyncMode = false;
        bool AsyncMode = false;

        ui64 NextKeyId = 1;

        THashSet<TActorId> ActiveUploaders;

    public:
        TS3Manager(TBlobDepot *self);
        ~TS3Manager();

        void Init(const NKikimrBlobDepot::TS3BackendSettings *settings);
        void TerminateAllActors();

        void Handle(TAutoPtr<IEventHandle> ev);

        void OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain);

        const TActorId& GetWrapperId() const { return WrapperId; }

        void AddTrashToCollect(TS3Locator locator);

        ui64 GetTotalS3TrashObjects() const { return TotalS3TrashObjects; }
        ui64 GetTotalS3TrashSize() const { return TotalS3TrashSize; }

    private: ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        class TTxPrepareWriteS3;
        friend class TBlobDepot;

        TS3Locator AllocateS3Locator(ui32 len);

    private: ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        class TUploaderActor;
        struct TEvUploadResult;

    private: ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        class TScannerActor;
        class TTxProcessScannedKeys;
        struct TEvScanFound;

        TActorId ScannerActorId;

        void RunScannerActor();
        void HandleScanner(TAutoPtr<IEventHandle> ev);

    private: ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        class TDeleterActor;
        class TTxDeleteTrashS3;
        struct TEvDeleteResult;

        static constexpr ui32 MaxDeletesInFlight = 3;
        static constexpr size_t MaxObjectsToDeleteAtOnce = 10;

        std::deque<TS3Locator> DeleteQueue; // items we are definitely going to delete (must be present in TrashS3)
        THashSet<TActorId> ActiveDeleters;
        ui32 NumDeleteTxInFlight = 0;
        ui64 TotalS3TrashObjects = 0;
        ui64 TotalS3TrashSize = 0;

        void RunDeletersIfNeeded();
        void HandleDeleter(TAutoPtr<IEventHandle> ev);
    };

} // NKikimr::NBlobDepot
