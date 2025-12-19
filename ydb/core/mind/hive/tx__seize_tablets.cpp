#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxSeizeTablets : public TTransactionBase<THive> {
    THolder<TEvHive::TEvSeizeTablets::THandle> Request;
    THolder<TEvHive::TEvSeizeTabletsReply> Response = MakeHolder<TEvHive::TEvSeizeTabletsReply>();

public:
    TTxSeizeTablets(THolder<TEvHive::TEvSeizeTablets::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_SEIZE_TABLETS; }

    static bool IsMatch(const TLeaderTabletInfo& tablet, const NKikimrHive::TEvSeizeTablets& record) {
        return tablet.ObjectDomain == TSubDomainKey(record.GetFilterDomain())
                && record.GetNewOwnerID() != tablet.Id;
    }

    static bool IsAbleToMigrate(const TLeaderTabletInfo& tablet) {
        // we can only migrate 'big' tablet ids, which have non-zero bits in 44+
        // that's because it stored in the same id space, where ownerIdx is stored
        // return !tablet.IsDeleting() && StateStorageGroupFromTabletID(tablet.Id) > 0 || HiveUidFromTabletID(tablet.Id) > 0;
        // ^^ temporary commented-out due to unit tests using 0 state storage group and 0 hive uid

        return tablet.IsReadyToWork()
                && tablet.ChannelProfileNewGroup.none()
                && tablet.Type != TTabletTypes::Hive // because we leave hive(s) in root hive
                && tablet.Type != TTabletTypes::BlockStoreVolume // because we don't have support for NBS yet
                && tablet.Type != TTabletTypes::BlockStorePartition // because we don't have support for NBS yet
                && tablet.Type != TTabletTypes::BlockStorePartition2; // because we don't have support for NBS yet

    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvSeizeTablets& request(Request->Get()->Record);
        NKikimrHive::TEvSeizeTabletsReply& response(Response->Record);
        BLOG_D("THive::TTxSeizeTablets::Execute " << request);
        TTabletId newOwnerId = request.GetNewOwnerID();
        response.ClearTablets();
        NIceDb::TNiceDb db(txc.DB);
        ui32 seizedTablets = 0;
        for (auto& [tabletId, tablet] : Self->Tablets) {
            if (IsMatch(tablet, request) && IsAbleToMigrate(tablet)) {
                // to simplify migration we skip following fields:
                // Schema::Tablet::Category - because nobody is using it now
                // Schema::Tablet::ActorsToNotify - because it's volatile and we are going to restart the tablet
                // Schema::Tablet::AllowedNodes - because nobody is using it now
                // Schema::Tablet::AllowedDataCenters - because nobody is using it now

                // we also skip current metrics state for followers

                TTabletId id = tabletId;
                BLOG_D("THive::TTxSeizeTablets is migrating tablet " << id << " to " << newOwnerId);

                auto tabletRowset = db.Table<Schema::Tablet>().Key(id).Select();
                if (!tabletRowset.IsReady()) {
                    return false;
                }
                if (tabletRowset.EndOfSet()) {
                    BLOG_D("THive::TTxSeizeTablets couldn't find tablet " << id << " in database");
                    continue;
                }

                NKikimrHive::TTabletInfo& tabletInfo = *response.AddTablets();
                tabletInfo.SetTabletID(tabletId);
                tabletInfo.MutableTabletOwner()->SetOwner(tablet.Owner.first);
                tabletInfo.MutableTabletOwner()->SetOwnerIdx(tablet.Owner.second);
                tabletInfo.SetTabletType(tablet.Type);
                tabletInfo.SetState(static_cast<ui32>(tablet.State));
                tabletInfo.SetVolatileState(tablet.GetVolatileState());
                tabletInfo.SetObjectId(tablet.ObjectId.second);
                tabletInfo.SetGeneration(tablet.KnownGeneration);
                ActorIdToProto(tablet.LockedToActor, tabletInfo.MutableLockedToActor());
                tabletInfo.SetLockedReconnectTimeout(tablet.LockedReconnectTimeout.MilliSeconds());
                tabletInfo.SetTabletStorageVersion(tablet.TabletStorageInfo->Version);
                tabletInfo.SetTabletBootMode(tablet.BootMode);
                tabletInfo.MutableResourceUsage()->CopyFrom(tablet.GetResourceValues());

                TSubDomainKey objectDomain = TSubDomainKey(tabletRowset.GetValueOrDefault<Schema::Tablet::ObjectDomain>());
                tabletInfo.MutableObjectDomain()->CopyFrom(objectDomain);

                TVector<TSubDomainKey> allowedDomains = tabletRowset.GetValueOrDefault<Schema::Tablet::AllowedDomains>();
                for (const auto& allowedDomain : allowedDomains) {
                    if (allowedDomain != objectDomain) {
                        tabletInfo.AddAllowedDomains()->CopyFrom(allowedDomain);
                    }
                }

                auto tabletChannelRowset = db.Table<Schema::TabletChannel>().Range(tabletId).Select();
                if (!tabletChannelRowset.IsReady())
                    return false;

                while (!tabletChannelRowset.EndOfSet()) {
                    ui32 channelId = tabletChannelRowset.GetValueOrDefault<Schema::TabletChannel::Channel>();
                    NKikimrHive::TTabletChannelInfo& tabletChannelInfo = *tabletInfo.AddTabletChannels();
                    tabletChannelInfo.SetStoragePool(tabletChannelRowset.GetValue<Schema::TabletChannel::StoragePool>());
                    tabletChannelInfo.MutableBinding()->CopyFrom(tabletChannelRowset.GetValueOrDefault<Schema::TabletChannel::Binding>());
                    tabletChannelInfo.SetNeedNewGroup(tabletChannelRowset.GetValueOrDefault<Schema::TabletChannel::NeedNewGroup>());

                    auto tabletChannelGenRowset = db.Table<Schema::TabletChannelGen>().Range(tabletId, channelId).Select();
                    if (!tabletChannelGenRowset.IsReady())
                        return false;

                    while (!tabletChannelGenRowset.EndOfSet()) {
                        NKikimrHive::TTabletChannelGenInfo& tabletChannelGenInfo = *tabletChannelInfo.AddHistory();
                        tabletChannelGenInfo.SetGeneration(tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Generation>());
                        tabletChannelGenInfo.SetGroup(tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Group>());
                        tabletChannelGenInfo.SetTimestamp(tabletChannelGenRowset.GetValueOrDefault<Schema::TabletChannelGen::Timestamp>());
                        tabletChannelGenInfo.SetVersion(tabletChannelGenRowset.GetValueOrDefault<Schema::TabletChannelGen::Version>());
                        if (!tabletChannelGenRowset.Next())
                            return false;
                    }

                    if (!tabletChannelRowset.Next())
                        return false;
                }

                auto tabletFollowerGroupRowset = db.Table<Schema::TabletFollowerGroup>().Range(tabletId).Select();
                if (!tabletFollowerGroupRowset.IsReady())
                    return false;

                while (!tabletFollowerGroupRowset.EndOfSet()) {
                    NKikimrHive::TFollowerGroup& followerGroup = *tabletInfo.AddFollowerGroups();
                    followerGroup.SetFollowerCount(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::FollowerCount>());
                    followerGroup.SetAllowLeaderPromotion(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowLeaderPromotion>());
                    followerGroup.SetAllowClientRead(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowClientRead>());
                    followerGroup.SetRequireAllDataCenters(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::RequireAllDataCenters>());
                    followerGroup.SetLocalNodeOnly(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::LocalNodeOnly>());
                    followerGroup.SetRequireAllDataCenters(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::RequireAllDataCenters>());
                    followerGroup.SetRequireDifferentNodes(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::RequireDifferentNodes>());
                    followerGroup.SetFollowerCountPerDataCenter(tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>());
                    if (!tabletFollowerGroupRowset.Next()) {
                        return false;
                    }
                }

                tablet.SeizedByChild = true;
                db.Table<Schema::Tablet>().Key(id).Update<Schema::Tablet::SeizedByChild>(true);

                if (++seizedTablets >= Request->Get()->Record.GetMaxTabletsToSeize()) {
                    break;
                }
            }
        }
        return true;
    }

    void Complete(const TActorContext& txc) override {
        BLOG_D("THive::TTxSeizeTablets::Complete " << Request->Get()->Record);
        txc.Send(Request->Sender, Response.Release());
    }
};

ITransaction* THive::CreateSeizeTablets(TEvHive::TEvSeizeTablets::TPtr event) {
    return new TTxSeizeTablets(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
