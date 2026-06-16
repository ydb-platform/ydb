#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxReassignGroups : public TTransactionBase<THive> {
protected:
    TTabletId TabletId;
    TActorId Sender;
    std::bitset<MAX_TABLET_CHANNELS> ChannelProfileNewGroup;
    bool Async;

    TSideEffects SideEffects;

public:
    TTxReassignGroups(TTabletId tabletId,
                      const TActorId& sender,
                      const std::bitset<MAX_TABLET_CHANNELS>& channelProfileNewGroup,
                      bool async,
                      THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Sender(sender)
        , ChannelProfileNewGroup(channelProfileNewGroup)
        , Async(async)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REASSIGN_GROUPS; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            BLOG_D("THive::TTxReassignGroups(" << tablet->Id << "," << ChannelProfileNewGroup << ")::Execute");
            if (tablet->IsReadyToReassignTablet()) {
                NIceDb::TNiceDb db(txc.DB);
                tablet->ChannelProfileNewGroup |= ChannelProfileNewGroup;
                tablet->ActorsToNotify.push_back(Sender);
                db.Table<Schema::Tablet>().Key(tablet->Id).Update(
                            NIceDb::TUpdate<Schema::Tablet::ActorsToNotify>(tablet->ActorsToNotify),
                            NIceDb::TUpdate<Schema::Tablet::ReassignReason>(tablet->ChannelProfileReassignReason));

                const ui32 channels = tablet->GetChannelCount();
                for (ui32 channelId = 0; channelId < channels; ++channelId) {
                    if (ChannelProfileNewGroup.none() || ChannelProfileNewGroup.test(channelId)) {
                        db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update(NIceDb::TUpdate<Schema::TabletChannel::NeedNewGroup>(true));
                    }
                }
                if (Async) {
                    tablet->NotifyOnRestart("marked for reassign", SideEffects);
                } else {
                    tablet->State = ETabletState::GroupAssignment;
                    db.Table<Schema::Tablet>().Key(tablet->Id).Update<Schema::Tablet::State>(ETabletState::GroupAssignment);
                    tablet->InitiateAssignTabletGroups();
                }
            } else {
                BLOG_W("THive::TTxReassignGroups(" << tablet->Id << ")::Execute - tablet is not ready for group reassignment");
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxReassignGroups(" << TabletId << ")::Complete");
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateReassignGroups(TTabletId tabletId,
                                          const TActorId& actorToNotify,
                                          const std::bitset<MAX_TABLET_CHANNELS>& channelProfileNewGroup,
                                          bool async) {
    return new TTxReassignGroups(tabletId, actorToNotify, channelProfileNewGroup, async, this);
}

} // NHive
} // NKikimr
