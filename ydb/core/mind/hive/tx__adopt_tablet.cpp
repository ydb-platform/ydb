#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxAdoptTablet : public TTransactionBase<THive> {
    const ui64 TabletId;
    const ui64 PrevOwner;
    const ui64 PrevOwnerIdx;
    const TTabletTypes::EType TabletType;

    const ui64 Owner;
    const ui64 OwnerIdx;

    const TActorId Sender;
    const ui64 Cookie;

    TString Explain;
    NKikimrProto::EReplyStatus Status;

public:
    TTxAdoptTablet(NKikimrHive::TEvAdoptTablet &rec, const TActorId &sender, const ui64 cookie, THive *hive)
        : TBase(hive)
        , TabletId(rec.GetTabletID())
        , PrevOwner(rec.GetPrevOwner())
        , PrevOwnerIdx(rec.GetPrevOwnerIdx())
        , TabletType(rec.GetTabletType())
        , Owner(rec.GetOwner())
        , OwnerIdx(rec.GetOwnerIdx())
        , Sender(sender)
        , Cookie(cookie)
    {
        Y_ABORT_UNLESS(!!Sender);
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_ADOPT_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_D("THive::TTxAdoptTablet::Execute");
        NIceDb::TNiceDb db(txc.DB);

        const TOwnerIdxType::TValueType prevOwner(PrevOwner, PrevOwnerIdx);
        const TOwnerIdxType::TValueType newOwner(Owner, OwnerIdx);


        {// check if tablet is already adopted
            auto itOwner = Self->OwnerToTablet.find(newOwner);
            if (itOwner != Self->OwnerToTablet.end()) {
                const ui64 tabletId = itOwner->second;
                if (tabletId != TabletId) {
                    Explain = TStringBuilder() << "there is another tablet assotiated with the"
                                                  " prevOwner:prevOwnerIdx " << PrevOwner << ":" << PrevOwnerIdx <<
                                                  " tabletId is " << tabletId <<
                                                  " was TabletId is " << TabletId;
                    Status = NKikimrProto::EReplyStatus::RACE;
                    return true;
                }

                TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
                if (tablet != nullptr && tablet->Type != TabletType) {
                    Explain = "there is the tablet with different type assotiated with the (owner; ownerIdx)";
                    Status = NKikimrProto::EReplyStatus::RACE;
                    return true;
                }

                if (tablet != nullptr) {
                    Explain = "it seems like the tablet aleready adopted";
                    Status = NKikimrProto::EReplyStatus::ALREADY;
                    return true;
                }
            }
        }

        auto itOwner = Self->OwnerToTablet.find(prevOwner);
        if (itOwner == Self->OwnerToTablet.end()) {
            Explain = "the tablet isn't found";
            Status = NKikimrProto::EReplyStatus::NODATA;
            return true;
        }

        const ui64 tabletId = itOwner->second;
        if (tabletId != TabletId) {
            Explain = TStringBuilder() << "there is another tablet assotiated with the"
                                          " prevOwner:prevOwnerIdx " << PrevOwner << ":" << PrevOwnerIdx <<
                                          " tabletId is " << tabletId <<
                                          " was TabletId is " << TabletId;
            Status = NKikimrProto::EReplyStatus::ERROR;
            return true;
        }

        TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
        if (tablet != nullptr && tablet->Type != TabletType) { // tablet is the same
            Explain = "there is the tablet with different type assotiated with the (preOwner; prevOwnerIdx)";
            TStringBuilder() << "there is tablet with different assotiated with the"
                                                      " prevOwner:prevOwnerIdx " << PrevOwner << ":" << PrevOwnerIdx <<
                                                      " type is " << TabletType <<
                                                      " was TabletId is " << tablet->Type;
            Status = NKikimrProto::EReplyStatus::ERROR;
            return true;
        }

        db.Table<Schema::Tablet>().Key(TabletId).Update(NIceDb::TUpdate<Schema::Tablet::Owner>(newOwner));

        Self->OwnerToTablet[newOwner] = itOwner->second;
        Self->OwnerToTablet.erase(prevOwner);

        Explain = "we did it";
        this->Status = NKikimrProto::OK;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxAdoptTablet::Complete TabletId: " << TabletId <<
               " Status: " << NKikimrProto::EReplyStatus_Name(Status) <<
               " Explain: " << Explain);

        ctx.Send(Sender, new TEvHive::TEvAdoptTabletReply(Status, TabletId, Owner, OwnerIdx, Explain, Self->TabletID()), 0, Cookie);;
    }

};

ITransaction* THive::CreateAdoptTablet(NKikimrHive::TEvAdoptTablet &rec, const TActorId &sender, const ui64 cookie) {
    return new TTxAdoptTablet(rec, sender, cookie, this);
}

} // NHive
} // NKikimr
