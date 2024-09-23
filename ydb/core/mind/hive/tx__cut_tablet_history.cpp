#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxCutTabletHistory : public TTransactionBase<THive> {
    TEvHive::TEvCutTabletHistory::TPtr Event;
public:
    TTxCutTabletHistory(TEvHive::TEvCutTabletHistory::TPtr& ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_CUT_TABLET_HISTORY; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        TEvHive::TEvCutTabletHistory* msg = Event->Get();
        auto tabletId = msg->Record.GetTabletID();
        BLOG_D("THive::TTxCutTabletHistory::Execute(" << tabletId << ")");
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
        if (tablet != nullptr && tablet->IsReadyToReassignTablet()) {
            auto channel = msg->Record.GetChannel();
            Y_ABORT_UNLESS(channel < tablet->TabletStorageInfo->Channels.size());
            TTabletChannelInfo& channelInfo = tablet->TabletStorageInfo->Channels[channel];
            auto fromGeneration = msg->Record.GetFromGeneration();
            auto groupId = msg->Record.GetGroupID();
            auto it = std::find(
                        channelInfo.History.begin(),
                        channelInfo.History.end(),
                        TTabletChannelInfo::THistoryEntry(fromGeneration, groupId));
            if (it != channelInfo.History.end()) {
                tablet->DeletedHistory.emplace_back(channel, *it);
                channelInfo.History.erase(it);
                /* to be safe, don't do it just yet
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::TabletChannelGen>().Key(tabletId, channel, fromGeneration).Delete();*/
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* THive::CreateCutTabletHistory(TEvHive::TEvCutTabletHistory::TPtr& ev) {
    return new TTxCutTabletHistory(ev, this);
}

} // NHive
} // NKikimr
