#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxUpdateTabletsObject : public TTransactionBase<THive> {
    TEvHive::TEvUpdateTabletsObject::TPtr Event;
    TSideEffects SideEffects;

public:
    TTxUpdateTabletsObject(TEvHive::TEvUpdateTabletsObject::TPtr ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_UPDATE_TABLETS_OBJECT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TEvHive::TEvUpdateTabletsObject* msg = Event->Get();
        auto objectId = msg->Record.GetObjectId();

        YDB_LOG_DEBUG("THive::TTxUpdateTabletsObject::Execute(",
            {"GetLogPrefix", GetLogPrefix()},
            {"objectId", objectId});

        NIceDb::TNiceDb db(txc.DB);
        ui64 tabletsUpdated = 0;
        THive::TAggregateMetrics* newObjectMetrics = nullptr;
        for (auto tabletId : msg->Record.GetTabletIds()) {
            auto tablet = Self->FindTablet(tabletId);
            if (tablet == nullptr) {
                continue;
            }
            auto node = tablet->GetNode();
            auto oldObject = tablet->GetObjectId();

            if (tablet->HasCounter() && node != nullptr) {
                Self->UpdateObjectCount(*tablet, *node, -1);
            }
            tablet->ObjectId.second = objectId;
            if (tablet->HasCounter() && node != nullptr) {
                Self->UpdateObjectCount(*tablet, *node, +1);
            }

            auto newObject = tablet->GetObjectId(); // It should be the same on every iteration
            if (oldObject == newObject) {
                continue;
            }
            ++tabletsUpdated;
            if (!newObjectMetrics) {
                newObjectMetrics = &Self->ObjectToTabletMetrics[newObject];
            }

            newObjectMetrics->AggregateDiff({}, tablet->GetResourceValues(), tablet);
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
                node->TabletsOfObject[newObject].emplace(tablet);
            }

            db.Table<Schema::Tablet>().Key(tabletId).Update<Schema::Tablet::ObjectID>(objectId);
        }
        if (newObjectMetrics) {
            newObjectMetrics->IncreaseCount(tabletsUpdated);
        }

        auto response = std::make_unique<TEvHive::TEvUpdateTabletsObjectReply>(NKikimrProto::OK);
        response->Record.SetTxId(Event->Get()->Record.GetTxId());
        response->Record.SetTxPartId(Event->Get()->Record.GetTxPartId());
        SideEffects.Send(Event->Sender, response.release(), 0, Event->Cookie);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("THive::TTxUpdateTabletsObject Complete",
            {"GetLogPrefix", GetLogPrefix()});
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateUpdateTabletsObject(TEvHive::TEvUpdateTabletsObject::TPtr ev) {
    return new TTxUpdateTabletsObject(std::move(ev), this);
}

} // NHive
} // NKikimr
