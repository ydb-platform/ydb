#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        std::weak_ptr<TToken> Token;
        TBlobDepot *Self;

        std::optional<ui64> SkipBlocksUpTo;
        std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
        std::optional<TLogoBlobID> SkipBlobsUpTo;

    public:
        TGroupAssimilator(TBlobDepot *self)
            : Token(self->Token)
            , Self(self)
        {
            Y_VERIFY(Self->Config.GetOperationMode() == NKikimrBlobDepot::EOperationMode::VirtualGroup);
        }

        void Bootstrap();
        void PassAway() override;
        STATEFN(StateFunc);

    private:
        void SendRequest();
        void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev);
        TString SerializeAssimilatorState() const;
    };

} // NKikimrBlobDepot::NBlobDepot
