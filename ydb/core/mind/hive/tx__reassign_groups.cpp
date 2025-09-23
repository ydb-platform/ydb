#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxReassignGroups : public TTransactionBase<THive> {
protected:
    TTabletId TabletId;
    TActorId Sender;
    std::bitset<MAX_TABLET_CHANNELS> ChannelProfileNewGroup;

public:
    TTxReassignGroups(TTabletId tabletId,
                      const TActorId& sender,
                      const std::bitset<MAX_TABLET_CHANNELS>& channelProfileNewGroup,
                      THive *hive)
        : TBase(hive)
        , TabletId(tabletId)
        , Sender(sender)
        , ChannelProfileNewGroup(channelProfileNewGroup)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REASSIGN_GROUPS; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        //SetTxType(NHive::TXTYPE_KILL_TABLET);
        TLeaderTabletInfo* tablet = Self->FindTablet(TabletId);
        if (tablet != nullptr) {
            BLOG_D("THive::TTxReassignGroups(" << tablet->Id << "," << ChannelProfileNewGroup << ")::Execute");
            if (tablet->IsReadyToReassignTablet()) {
                NIceDb::TNiceDb db(txc.DB);
                tablet->State = ETabletState::GroupAssignment;
                tablet->ChannelProfileNewGroup |= ChannelProfileNewGroup;
                tablet->ActorsToNotify.push_back(Sender);
                db.Table<Schema::Tablet>().Key(tablet->Id).Update(
                            NIceDb::TUpdate<Schema::Tablet::State>(ETabletState::GroupAssignment),
                            NIceDb::TUpdate<Schema::Tablet::ActorsToNotify>(tablet->ActorsToNotify),
                            NIceDb::TUpdate<Schema::Tablet::ReassignReason>(tablet->ChannelProfileReassignReason));

                const ui32 channels = tablet->GetChannelCount();
                for (ui32 channelId = 0; channelId < channels; ++channelId) {
                    if (ChannelProfileNewGroup.none() || ChannelProfileNewGroup.test(channelId)) {
                        db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update(NIceDb::TUpdate<Schema::TabletChannel::NeedNewGroup>(true));
                    }
                }
                tablet->InitiateAssignTabletGroups();
            } else {
                BLOG_W("THive::TTxReassignGroups(" << tablet->Id << ")::Execute - tablet is not ready for group reassignment");
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxReassignGroups(" << TabletId << ")::Complete");
    }
};

ITransaction* THive::CreateReassignGroups(TTabletId tabletId,
                                          const TActorId& actorToNotify,
                                          const std::bitset<MAX_TABLET_CHANNELS>& channelProfileNewGroup) {
    return new TTxReassignGroups(tabletId, actorToNotify, channelProfileNewGroup, this);
}

} // NHive
} // NKikimr
