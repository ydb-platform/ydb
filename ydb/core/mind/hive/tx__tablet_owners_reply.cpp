#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxTabletOwnersReply : public TTransactionBase<THive> {
    THolder<TEvHive::TEvTabletOwnersReply::THandle> Request;

public:
    TTxTabletOwnersReply(THolder<TEvHive::TEvTabletOwnersReply::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_TABLET_OWNERS_REPLY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvTabletOwnersReply& request(Request->Get()->Record);
        BLOG_D("THive::TTxTabletOwnersReply::Execute");
        NIceDb::TNiceDb db(txc.DB);
        for (const NKikimrHive::TTabletOwnerRecord& protoTabletOwner : request.GetTabletOwners()) {
            TOwnershipKeeper::TOwnerType ownerId = protoTabletOwner.GetOwnerID();
            TSequencer::TSequence seq(protoTabletOwner.GetBegin(), protoTabletOwner.GetEnd());
            if (Self->Keeper.AddOwnedSequence(ownerId, seq)) {
                BLOG_D("THive::TTxTabletOwnersReply::Execute - add new owned sequence ("
                    << seq.Begin << "," << seq.End << ") = " << ownerId);
                db.Table<Schema::TabletOwners>().Key(seq.Begin, seq.End).Update<Schema::TabletOwners::OwnerId>(ownerId);
            }
        }
        db.Table<Schema::State>().Key(TSchemeIds::State::TabletOwnersSynced).Update<Schema::State::Value>(true);
        Self->TabletOwnersSynced = true;
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxTabletOwnersReply::Complete");
    }
};

ITransaction* THive::CreateTabletOwnersReply(TEvHive::TEvTabletOwnersReply::TPtr event) {
    return new TTxTabletOwnersReply(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
