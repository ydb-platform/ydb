#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxUpdateTabletsObject : public TTransactionBase<THive> {
    TEvHive::TEvUpdateTabletsObject::TPtr Event;

public:
    TTxUpdateTabletsObject(TEvHive::TEvUpdateTabletsObject::TPtr ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLETS_OBJECT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TEvHive::TEvUpdateTabletsObject* msg = Event->Get();
        auto objectId = msg->Record.GetObjectId();

        BLOG_D("THive::TTxCutTabletHistory::Execute(" << objectId << ")");

        NIceDb::TNiceDb db(txc.DB);
        ui64 tabletsUpdated = 0;
        auto& newObjectMetrics = Self->ObjectToTabletMetrics[objectId];
        for (auto tabletId : msg->Record.GetTabletIds()) {
            auto tablet = Self->FindTablet(tabletId);
            if (tablet == nullptr) {
                continue;
            }
            auto oldObject = tablet->ObjectId;
            if (oldObject == objectId) {
                continue;
            }
            tablet->ObjectId = objectId;
            ++tabletsUpdated;

            newObjectMetrics.AggregateDiff({}, tablet->GetResourceValues(), tablet);
            if (auto itObj = Self->ObjectToTabletMetrics.find(oldObject); itObj != Self->ObjectToTabletMetrics.end()) {
                auto& oldObjectMetrics = itObj->second;
                oldObjectMetrics.DecreaseCount();
                if (oldObjectMetrics.Counter == 0) {
                    Self->ObjectToTabletMetrics.erase(itObj);
                } else {
                    oldObjectMetrics.AggregateDiff(tablet->GetResourceValues(), {}, tablet);
                }
            }

            if (auto node = tablet->GetNode(); node != nullptr) {
                node->TabletsOfObject[oldObject].erase(tablet);
                node->TabletsOfObject[objectId].emplace(tablet);
            }

            db.Table<Schema::Tablet>().Key(tabletId).Update<Schema::Tablet::ObjectID>(objectId);
        }
        newObjectMetrics.IncreaseCount(tabletsUpdated);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxUpdateTabletsObject Complete");
        ctx.Send(Event->Sender, new TEvHive::TEvUpdateTabletsObjectReply(NKikimrProto::OK), 0, Event->Cookie);
    }
};

ITransaction* THive::CreateUpdateTabletsObject(TEvHive::TEvUpdateTabletsObject::TPtr ev) {
    return new TTxUpdateTabletsObject(std::move(ev), this);
}

} // NHive
} // NKikimr
