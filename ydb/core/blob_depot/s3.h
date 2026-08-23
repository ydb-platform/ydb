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
        // A slot is held from PrepareWriteS3 until the HTTP upload finishes (CommitBlobSeq is
        // received, or the locator is discarded) — not until metadata commit completes.
        static constexpr ui32 SuccessesPerWriteConcurrencyStepUp = 3;

        TBackoff PutBackoff{TDuration::MilliSeconds(100), TDuration::Seconds(60)};
        TMonotonic PutThrottleUntil;
        bool PutWakeupScheduled = false;
        ui32 CurrentMaxWritesInFlight = Max<ui32>();
        ui32 ConsecutiveSuccessfulWriteBatches = 0;
        ui32 S3WritesInFlight = 0;
        std::deque<TEvBlobDepot::TEvPrepareWriteS3::TPtr> PendingPrepareWrites;

        // The configured ceiling (ICB, BlobDepotControls.S3MaxWritesInFlight) and the actually
        // enforced limit, which is the ceiling capped by what the adaptive limiter has recovered to
        // after the last SlowDown. Read fresh every time -- the ICB value may change under us.
        ui32 MaxWritesInFlight() const;
        ui32 EffectiveMaxWritesInFlight() const { return Min(CurrentMaxWritesInFlight, MaxWritesInFlight()); }

        void HandlePrepareWriteS3(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev);
        void NotifyPutSlowDown();
        void HandlePutThrottleWakeup();
        void RunPendingPrepareWritesIfPossible();
        void OnS3WriteInFlightAdded(ui32 count);
        void OnS3WriteInFlightRemoved(bool success, ui32 count = 1);

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

        // DeleteObjects is a multi-delete request (S3 allows up to 1000 keys per request), so the batch size
        // is what actually determines delete throughput. Every request occupies a thread of the shared AWS
        // executor pool for its whole duration, competing with puts, hence we prefer fewer larger batches.
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
        ui32 CurrentMaxDeletesInFlight = Max<ui32>();
        ui32 ConsecutiveSuccessfulDeleteBatches = 0;

        // Same split as for writes: configured ceiling vs. what the adaptive limiter allows now.
        ui32 MaxDeletesInFlight() const;
        ui32 EffectiveMaxDeletesInFlight() const { return Min(CurrentMaxDeletesInFlight, MaxDeletesInFlight()); }
        size_t MaxObjectsToDeleteAtOnce() const;

        void RunDeletersIfNeeded();
        void HandleDeleter(TAutoPtr<IEventHandle> ev);
        void HandleDeleteThrottleWakeup();
    };

} // NKikimr::NBlobDepot
