#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

#include <ydb/core/util/backoff.h>

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TS3Manager {
        TBlobDepot* const Self;
        // WrapperId is always the per-node router service id managed by NodeWarden
        // (acquired via TEvNodeWardenAcquireBlobDepotS3Router on Init, released on
        // TerminateAllActors). All S3 traffic is forwarded through the router.
        TActorId WrapperId;
        TActorId UploaderId;
        TString BasePath;
        TString Bucket;

        bool Enabled = false;
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

        void OnDataLoaded();

    private: ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        class TTxPrepareWriteS3;
        friend class TBlobDepot;

        TS3Locator AllocateS3Locator(ui32 len);

        // Throttling state for S3 SlowDown responses on agent put requests. Puts are issued by agents (one per
        // node), but throttling is centralized at the tablet by postponing TEvPrepareWriteS3Result.
        static constexpr ui32 MaxWritesInFlight = 32;
        static constexpr ui32 SuccessesPerWriteConcurrencyStepUp = 3;

        TBackoff PutBackoff{TDuration::MilliSeconds(100), TDuration::Seconds(60)};
        TMonotonic PutThrottleUntil;
        bool PutWakeupScheduled = false;
        ui32 CurrentMaxWritesInFlight = MaxWritesInFlight;
        ui32 ConsecutiveSuccessfulWriteBatches = 0;
        ui32 S3WritesInFlight = 0;
        std::deque<TEvBlobDepot::TEvPrepareWriteS3::TPtr> PendingPrepareWrites;

        void HandlePrepareWriteS3(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev);
        void NotifyPutSlowDown();
        void HandlePutThrottleWakeup();
        void RunPendingPrepareWritesIfPossible();
        void OnS3WriteInFlightAdded(ui32 count);
        void OnS3WriteInFlightRemoved(bool success);

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
        static constexpr ui32 SuccessesPerConcurrencyStepUp = 3;

        // items we are definitely going to delete (must be present in TrashS3)
        std::deque<TS3Locator> DeleteQueue;
        THashSet<TActorId> ActiveDeleters;
        ui32 NumDeleteTxInFlight = 0;
        ui64 TotalS3TrashObjects = 0;
        ui64 TotalS3TrashSize = 0;

        // Throttling state for S3 SlowDown responses on delete requests.
        TBackoff DeleteBackoff{TDuration::MilliSeconds(100), TDuration::Seconds(60)};
        TMonotonic DeleteThrottleUntil;
        bool DeleteWakeupScheduled = false;
        ui32 CurrentMaxDeletesInFlight = MaxDeletesInFlight;
        ui32 ConsecutiveSuccessfulDeleteBatches = 0;

        void RunDeletersIfNeeded();
        void HandleDeleter(TAutoPtr<IEventHandle> ev);
        void HandleDeleteThrottleWakeup();
    };

} // NKikimr::NBlobDepot
