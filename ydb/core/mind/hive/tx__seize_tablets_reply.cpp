#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxSeizeTabletsReply : public TTransactionBase<THive> {
    THolder<TEvHive::TEvSeizeTabletsReply::THandle> Request;
    TVector<TTabletId> TabletIds;

public:
    TTxSeizeTabletsReply(THolder<TEvHive::TEvSeizeTabletsReply::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_SEIZE_TABLETS_REPLY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvSeizeTabletsReply& request(Request->Get()->Record);
        BLOG_D("THive::TTxSeizeTabletsReply::Execute");
        NIceDb::TNiceDb db(txc.DB);
        for (const NKikimrHive::TTabletInfo& protoTabletInfo : request.GetTablets()) {
            TTabletId tabletId = protoTabletInfo.GetTabletID();
            std::pair<ui64, ui64> owner(protoTabletInfo.GetTabletOwner().GetOwner(), protoTabletInfo.GetTabletOwner().GetOwnerIdx());
            TLeaderTabletInfo& tablet = Self->GetTablet(tabletId);
            tablet.SetType(protoTabletInfo.GetTabletType());
            tablet.NodeId = 0;
            tablet.Node = nullptr;
            tablet.BecomeStopped();
            tablet.KnownGeneration = protoTabletInfo.GetGeneration();
            tablet.State = static_cast<ETabletState>(protoTabletInfo.GetState());
            tablet.Owner = owner;
            tablet.BootMode = protoTabletInfo.GetTabletBootMode();
            tablet.ObjectId = {owner.first, protoTabletInfo.GetObjectId()};

            TVector<TSubDomainKey> allowedDomains;
            for (const auto& allowedDomain : protoTabletInfo.GetAllowedDomains()) {
                allowedDomains.emplace_back(allowedDomain);
            }

            TSubDomainKey objectDomain(protoTabletInfo.GetObjectDomain());

            tablet.AssignDomains(objectDomain, allowedDomains);

            tablet.UpdateResourceUsage(protoTabletInfo.GetResourceUsage());
            tablet.BoundChannels.clear();
            tablet.TabletStorageInfo.Reset(new TTabletStorageInfo(tablet.Id, tablet.Type));
            tablet.TabletStorageInfo->TenantPathId = tablet.GetTenant();
            tablet.TabletStorageInfo->Version = protoTabletInfo.GetTabletStorageVersion();

            tablet.LockedToActor = ActorIdFromProto(protoTabletInfo.GetLockedToActor());
            tablet.LockedReconnectTimeout = TDuration::MilliSeconds(protoTabletInfo.GetLockedReconnectTimeout());
            if (Self->CurrentConfig.GetLockedTabletsSendMetrics() && tablet.LockedToActor) {
                tablet.BecomeUnknown(tablet.Hive.FindNode(tablet.LockedToActor.NodeId()));
            }

            db.Table<Schema::Tablet>().Key(tabletId).Update(
                        NIceDb::TUpdate<Schema::Tablet::Owner>(owner),
                        NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(protoTabletInfo.GetGeneration()),
                        NIceDb::TUpdate<Schema::Tablet::TabletType>(protoTabletInfo.GetTabletType()),
                        NIceDb::TUpdate<Schema::Tablet::State>(static_cast<ETabletState>(protoTabletInfo.GetState())),
                        NIceDb::TUpdate<Schema::Tablet::LeaderNode>(protoTabletInfo.GetNodeID()),
                        //NIceDb::TUpdate<Schema::Tablet::Category>(),
                        //NIceDb::TUpdate<Schema::Tablet::AllowedNodes>(),
                        //NIceDb::TUpdate<Schema::Tablet::AllowedDataCenters>(),
                        NIceDb::TUpdate<Schema::Tablet::TabletStorageVersion>(protoTabletInfo.GetTabletStorageVersion()),
                        NIceDb::TUpdate<Schema::Tablet::ObjectID>(protoTabletInfo.GetObjectId()),
                        //NIceDb::TUpdate<Schema::Tablet::ActorsToNotify>(),
                        NIceDb::TUpdate<Schema::Tablet::AllowedDomains>(allowedDomains),
                        NIceDb::TUpdate<Schema::Tablet::BootMode>(protoTabletInfo.GetTabletBootMode()),
                        NIceDb::TUpdate<Schema::Tablet::LockedToActor>(ActorIdFromProto(protoTabletInfo.GetLockedToActor())),
                        NIceDb::TUpdate<Schema::Tablet::LockedReconnectTimeout>(protoTabletInfo.GetLockedReconnectTimeout()),
                        NIceDb::TUpdate<Schema::Tablet::ObjectDomain>(protoTabletInfo.GetObjectDomain()),
                        NIceDb::TUpdate<Schema::Tablet::NeedToReleaseFromParent>(true));

            TVector<TTabletChannelInfo>& tabletChannels = tablet.TabletStorageInfo->Channels;
            ui16 channelId = 0;
            for (const auto& protoTabletChannel : protoTabletInfo.GetTabletChannels()) {
                db.Table<Schema::TabletChannel>().Key(tabletId, channelId).Update(
                            NIceDb::TUpdate<Schema::TabletChannel::NeedNewGroup>(protoTabletChannel.GetNeedNewGroup()),
                            NIceDb::TUpdate<Schema::TabletChannel::StoragePool>(protoTabletChannel.GetStoragePool()),
                            NIceDb::TUpdate<Schema::TabletChannel::Binding>(protoTabletChannel.GetBinding()));

                tablet.BoundChannels.emplace_back();
                NKikimrStoragePool::TChannelBind& bind = tablet.BoundChannels.back();
                bind = protoTabletChannel.GetBinding();
                bind.SetStoragePoolName(protoTabletChannel.GetStoragePool());
                Self->InitDefaultChannelBind(bind);

                if (protoTabletChannel.GetNeedNewGroup()) {
                    tablet.ChannelProfileNewGroup.set(channelId);
                }

                tabletChannels.emplace_back();
                tabletChannels.back().Channel = channelId;

                TTabletChannelInfo& channel = tablet.TabletStorageInfo->Channels[channelId];

                for (const auto& protoTabletChannelGen : protoTabletChannel.GetHistory()) {
                    ui64 generation = protoTabletChannelGen.GetGeneration();
                    ui32 groupId = protoTabletChannelGen.GetGroup();
                    TInstant timestamp = TInstant::MilliSeconds(protoTabletChannelGen.GetTimestamp());
                    db.Table<Schema::TabletChannelGen>().Key(tabletId, channelId, generation).Update(
                                NIceDb::TUpdate<Schema::TabletChannelGen::Group>(groupId),
                                NIceDb::TUpdate<Schema::TabletChannelGen::Version>(protoTabletChannelGen.GetVersion()),
                                NIceDb::TUpdate<Schema::TabletChannelGen::Timestamp>(timestamp.MilliSeconds()));

                    channel.History.emplace_back(generation, groupId, timestamp);
                }

                ++channelId;
            }

            tablet.AcquireAllocationUnits();

            for (const auto& protoFollowerGroup : protoTabletInfo.GetFollowerGroups()) {
                TFollowerGroup& followerGroup = tablet.AddFollowerGroup();
                followerGroup = protoFollowerGroup;

                TVector<ui32> allowedDataCenters;
                for (const TDataCenterId& dc : followerGroup.NodeFilter.AllowedDataCenters) {
                    allowedDataCenters.push_back(DataCenterFromString(dc));
                }
                db.Table<Schema::TabletFollowerGroup>().Key(tabletId, followerGroup.Id).Update(
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerGroup.GetRawFollowerCount()),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(followerGroup.AllowLeaderPromotion),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(followerGroup.AllowClientRead),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedNodes>(followerGroup.NodeFilter.AllowedNodes),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenters>(allowedDataCenters),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenterIds>(followerGroup.NodeFilter.AllowedDataCenters),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireAllDataCenters>(followerGroup.RequireAllDataCenters),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireDifferentNodes>(followerGroup.RequireDifferentNodes),
                            NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>(followerGroup.FollowerCountPerDataCenter));

                for (ui32 i = 0; i < followerGroup.GetComputedFollowerCount(Self->GetDataCenters()); ++i) {
                    TFollowerTabletInfo& follower = tablet.AddFollower(followerGroup);
                    db.Table<Schema::TabletFollowerTablet>().Key(tabletId, follower.Id).Update(
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::GroupID>(follower.FollowerGroup.Id),
                                NIceDb::TUpdate<Schema::TabletFollowerTablet::FollowerNode>(0));
                    follower.InitTabletMetrics();
                    follower.BecomeStopped();
                }
            }

            ui64 uniqPart = UniqPartFromTabletID(tabletId);
            TOwnershipKeeper::TOwnerType newOwnerId = Self->TabletID();
            TSequencer::TSequence pointSeq(uniqPart);

            db.Table<Schema::TabletOwners>().Key(pointSeq.Begin, pointSeq.End).Update<Schema::TabletOwners::OwnerId>(newOwnerId);
            Self->Keeper.AddOwnedSequence(newOwnerId, pointSeq);

            if (Self->OwnerToTablet.emplace(owner, tabletId).second) {
                Self->ObjectToTabletMetrics[tablet.ObjectId].IncreaseCount();
                Self->TabletTypeToTabletMetrics[tablet.Type].IncreaseCount();
            }

            TabletIds.emplace_back(tabletId);
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_D("THive::TTxSeizeTabletsReply::Complete");
        if (!TabletIds.empty()) {
            THolder<TEvHive::TEvReleaseTablets> request(new TEvHive::TEvReleaseTablets());
            request->Record.SetNewOwnerID(Self->TabletID());
            for (TTabletId tabletId : TabletIds) {
                request->Record.AddTabletIDs(tabletId);
            }
            ctx.Send(Request->Sender, request.Release());
        } else {
            BLOG_D("Migration complete (" << Self->MigrationProgress << " tablets migrated)");
            Self->MigrationState = NKikimrHive::EMigrationState::MIGRATION_COMPLETE;
        }
    }
};

ITransaction* THive::CreateSeizeTabletsReply(TEvHive::TEvSeizeTabletsReply::TPtr event) {
    return new TTxSeizeTabletsReply(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
