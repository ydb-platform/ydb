#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        struct TEvPrivate {
            enum {
                EvResume = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvTxComplete,
            };
        };

        std::weak_ptr<TToken> Token;
        TBlobDepot *Self;

        std::optional<ui64> SkipBlocksUpTo;
        std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
        std::optional<TLogoBlobID> SkipBlobsUpTo;

        std::optional<TLogoBlobID> LastScannedKey;
        bool EntriesToProcess = false;

        static constexpr ui32 MaxSizeToQuery = 10'000'000;

        ui32 NumPutsInFlight = 0;

        TActorId PipeId;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_ASSIMILATOR_ACTOR;
        }

        TGroupAssimilator(TBlobDepot *self)
            : Token(self->Token)
            , Self(self)
        {
            Y_VERIFY(Self->Config.HasVirtualGroupId());
        }

        void Bootstrap();
        void PassAway() override;
        STATEFN(StateFunc);

    private:
        void Action();
        void SendAssimilateRequest();
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev);
        void ScanDataForCopying();
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);
        void HandleTxComplete();
        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev);
        void IssueCollects();
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void OnCopyDone();
        void CreatePipe();
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerGroupDecommittedResponse::TPtr ev);
        TString SerializeAssimilatorState() const;
    };

} // NKikimrBlobDepot::NBlobDepot
