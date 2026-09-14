#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        struct TEvPrivate {
            enum {
                EvResume = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvResumeScanDataForPlanning,
                EvResumeScanDataForCopying,
                EvTxComplete,
                EvUpdateBytesCopiedQ,
            };
        };

        std::weak_ptr<TToken> Token;
        TBlobDepot *Self;

        std::optional<ui64> SkipBlocksUpTo;
        std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
        std::optional<TLogoBlobID> SkipBlobsUpTo;

        std::optional<TLogoBlobID> LastScannedKey;
        bool EntriesToProcess = false;

        static constexpr ui32 MaxSizeToQuery = 16'000'000;

        struct TGetBatch {
            ui32 PutsPending = 0;
            std::vector<TData::TAssimilatedBlobInfo> AssimilatedBlobs;
            std::vector<TLogoBlobID> BlobIds;
        };
        static constexpr ui32 MaxGetsUnprocessed = 5;
        ui64 NextGetId = 1;
        THashMap<ui64, TGetBatch> Gets;

        std::deque<TLogoBlobID> ScanQ;
        ui32 TotalSize = 0;

        TActorId PipeId;

        ui64 NextPutId = 1;
        THashMap<ui64, std::tuple<TData::TKey, ui64>> Puts;

        bool ActionInProgress = false;
        bool ResumeScanDataForCopyingInFlight = false;

        std::optional<TLogoBlobID> LastPlanScannedKey;
        bool PlanningComplete = false;
        bool ResumeScanDataForPlanningInFlight = false;

        std::deque<std::tuple<TMonotonic, ui64>> BytesCopiedQ;

        ui32 ExpectedPerGenerationCounter = 0;
        THashMap<std::tuple<ui64, ui8>, std::deque<std::tuple<TGenStep, ui32, ui32>>> LeastBlobQueue;
        std::map<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> CollectGarbageQ;
        ui32 CollectGarbageInFlight = 0;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_ASSIMILATOR_ACTOR;
        }

        TGroupAssimilator(TBlobDepot *self)
            : Token(self->Token)
            , Self(self)
        {
            Y_ABORT_UNLESS(Self->Config.HasVirtualGroupId());
        }

        void Bootstrap();
        void PassAway() override;
        STATEFN(StateFunc);

    private:
        void Action();
        void SendAssimilateRequest();
        void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev);
        void ScanDataForPlanning();
        void HandleResumeScanDataForPlanning();
        void ScanDataForCopying();
        void HandleResumeScanDataForCopying();
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);
        void HandleTxComplete(TAutoPtr<IEventHandle> ev);
        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev);
        void Handle(TEvBlobStorage::TEvCollectGarbage::TPtr ev);
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void ProcessCollectGarbageQ();
        void OnCopyDone();
        void CreatePipe();
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerGroupDecommittedResponse::TPtr ev);
        TString SerializeAssimilatorState() const;
        void UpdateAssimilatorPosition() const;
        void UpdateBytesCopiedQ();
    };

} // NKikimrBlobDepot::NBlobDepot
