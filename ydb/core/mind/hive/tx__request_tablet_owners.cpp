#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxRequestTabletOwners : public TTransactionBase<THive> {
    THolder<TEvHive::TEvRequestTabletOwners::THandle> Request;
    THolder<TEvHive::TEvTabletOwnersReply> Response;

public:
    TTxRequestTabletOwners(THolder<TEvHive::TEvRequestTabletOwners::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
        , Response(new TEvHive::TEvTabletOwnersReply())
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REQUEST_TABLET_OWNERS; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        BLOG_D("THive::TTxRequestTabletOwners::Execute");
        auto ownerId = Request->Get()->Record.GetOwnerID();
        std::vector<TSequencer::TSequence> sequences;
        Self->Keeper.GetOwnedSequences(ownerId, sequences);
        BLOG_D("THive::TTxRequestTabletOwners - replying with " << sequences.size() << " sequences");
        for (const auto& seq : sequences) {
            auto* tabletOwners = Response->Record.AddTabletOwners();
            tabletOwners->SetOwnerID(ownerId);
            tabletOwners->SetBegin(seq.Begin);
            tabletOwners->SetEnd(seq.End);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxRequestTabletOwners::Complete");
        Self->Send(Request->Sender, Response.Release());
    }
};

ITransaction* THive::CreateRequestTabletOwners(TEvHive::TEvRequestTabletOwners::TPtr event) {
    return new TTxRequestTabletOwners(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
