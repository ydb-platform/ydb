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

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        TEvHive::TEvCutTabletHistory* msg = Event->Get();
        auto tabletId = msg->Record.GetTabletID();
        BLOG_D("THive::TTxCutTabletHistory::Execute(" << tabletId << ")");
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
        if (tablet != nullptr && tablet->IsReadyToReassignTablet() && Self->IsCutHistoryAllowed(tablet->Type)) {
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
                auto& histogram = Self->TabletCounters->Percentile()[NHive::COUNTER_TABLET_CHANNEL_HISTORY_SIZE];
                histogram.DecrementFor(channelInfo.History.size());
                Self->TabletCounters->Cumulative()[NHive::COUNTER_HISTORY_CUT].Increment(1);
                auto mode = msg->Record.GetCutHistoryMode();
                tablet->DeletedHistory[channel].emplace(*it, tablet->KnownGeneration, mode);
                channelInfo.History.erase(it);
                histogram.IncrementFor(channelInfo.History.size());
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::TabletChannelGen>().Key(tabletId, channel, fromGeneration).Update<Schema::TabletChannelGen::DeletedAtGeneration, Schema::TabletChannelGen::CutHistoryMode>(tablet->KnownGeneration, mode);
            } else {
                BLOG_W("THive::TTxCutTabletHistory::Execute(" << tabletId << "): history entry not found");
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TTxCommitCutTabletHistory : public TTransactionBase<THive> {
    TEvHive::TEvCommitCutTabletHistory::TPtr Event;
public:
    TTxCommitCutTabletHistory(TEvHive::TEvCommitCutTabletHistory::TPtr ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_COMMIT_CUT_TABLET_HISTORY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        auto* msg = Event->Get();
        auto tabletId = msg->Record.GetTabletID();
        BLOG_D("THive::TTxCommitCutTabletHistory::Execute(" << tabletId << ")");
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
        if (tablet != nullptr && Self->IsCutHistoryAllowed(tablet->Type)) {
            for (auto channel : msg->Record.GetChannels()) {
                if (channel >= tablet->DeletedHistory.size()) {
                    continue;
                }
                auto& deletedHistory = tablet->DeletedHistory[channel];
                while (!deletedHistory.empty()) {
                    const auto& entry = deletedHistory.front();
                    db.Table<Schema::TabletChannelGen>().Key(tabletId, channel, entry.Entry.FromGeneration).Delete();
                    deletedHistory.pop();
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TTxRevertCutTabletHistory : public TTransactionBase<THive> {
    TEvHive::TEvRevertCutTabletHistory::TPtr Event;
public:
    TTxRevertCutTabletHistory(TEvHive::TEvRevertCutTabletHistory::TPtr ev, THive* hive)
        : TBase(hive)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REVERT_CUT_TABLET_HISTORY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        auto* msg = Event->Get();
        auto tabletId = msg->Record.GetTabletID();
        BLOG_D("THive::TTxRevertCutTabletHistory::Execute(" << tabletId << ")");
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
        if (tablet != nullptr && Self->IsCutHistoryAllowed(tablet->Type)) {
            for (auto channel : msg->Record.GetChannels()) {
                if (channel >= tablet->DeletedHistory.size()) {
                    continue;
                }
                Self->TabletCounters->Cumulative()[NHive::COUNTER_HISTORY_RESTORED].Increment(tablet->DeletedHistory[channel].size());
                auto& histogram = Self->TabletCounters->Percentile()[NHive::COUNTER_TABLET_CHANNEL_HISTORY_SIZE];
                const auto& channelInfo = tablet->TabletStorageInfo->Channels[channel];
                histogram.DecrementFor(channelInfo.History.size());
                tablet->RestoreDeletedHistory(txc, channel);
                histogram.IncrementFor(channelInfo.History.size());
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

ITransaction* THive::CreateCutTabletHistory(TEvHive::TEvCutTabletHistory::TPtr& ev) {
    return new TTxCutTabletHistory(ev, this);
}

ITransaction* THive::CreateCommitCutTabletHistory(TEvHive::TEvCommitCutTabletHistory::TPtr ev) {
    return new TTxCommitCutTabletHistory(std::move(ev), this);
}

ITransaction* THive::CreateRevertCutTabletHistory(TEvHive::TEvRevertCutTabletHistory::TPtr ev) {
    return new TTxRevertCutTabletHistory(std::move(ev), this);
}

} // NHive
} // NKikimr
