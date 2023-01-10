#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr::NHive {

class TTxReassignGroupsOnDecommit : public TTransactionBase<THive> {
    const ui32 GroupId;
    std::unique_ptr<IEventHandle> Reply;

public:
    TTxReassignGroupsOnDecommit(ui32 groupId, std::unique_ptr<IEventHandle> reply, THive *hive)
        : TBase(hive)
        , GroupId(groupId)
        , Reply(std::move(reply))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REASSIGN_GROUPS_ON_DECOMMIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        for (auto& [tabletId, tablet] : Self->Tablets) {
            if (tablet.IsDeleting()) {
                continue;
            }

            const TTabletId tabletId_ = tabletId;
            bool changed = false;
            ui32 numChannels = 0;

            auto& channels = tablet.TabletStorageInfo->Channels;
            for (ui32 channel = 0; channel < tablet.GetChannelCount(); ++channel) {
                auto *entry = channel < channels.size() ? channels[channel].LatestEntry() : nullptr;
                if (!entry) {
                    BLOG_W("TTxReassignGroupsOnDecommit entry not found TabletId# " << tabletId_
                        << " channel# " << channel << " GroupId# " << GroupId);
                    continue;
                } else if (entry->GroupID != GroupId) {
                    continue;
                }

                tablet.ChannelProfileNewGroup.set(channel);
                db.Table<Schema::TabletChannel>().Key(tabletId, channel).Update(
                    NIceDb::TUpdate<Schema::TabletChannel::NeedNewGroup>(true)
                );
                ++numChannels;

                if (changed || !tablet.IsReadyToReassignTablet()) {
                    BLOG_D("TTxReassignGroupsOnDecommit tablet is not ready for reassignment TabletId# " << tabletId_);
                    continue;
                }

                changed = true;
                tablet.State = ETabletState::GroupAssignment;
                tablet.ChannelProfileReassignReason = NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO;
                db.Table<Schema::Tablet>().Key(tabletId).Update(
                    NIceDb::TUpdate<Schema::Tablet::State>(tablet.State),
                    NIceDb::TUpdate<Schema::Tablet::ReassignReason>(tablet.ChannelProfileReassignReason)
                );
            }

            if (changed) {
                BLOG_D("TTxReassignGroupsOnDecommit tablet reassigned TabletId# " << tabletId_ << " numChannels# " << numChannels);
                tablet.InitiateAssignTabletGroups();
            }
		}

        return true;
    }

    void Complete(const TActorContext&) override {
        TActivationContext::Send(Reply.release());
    }
};

ITransaction* THive::CreateReassignGroupsOnDecommit(ui32 groupId, std::unique_ptr<IEventHandle> reply) {
    return new TTxReassignGroupsOnDecommit(groupId, std::move(reply), this);
}

} // NKikimr::NHive
