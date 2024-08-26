#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxLoadEverything : public TTransactionBase<THive> {
public:
    TTxLoadEverything(THive *hive)
        : TBase(hive)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_LOAD_EVERYTHING; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        BLOG_NOTICE("THive::TTxLoadEverything::Execute");

        TAppData* appData = AppData();
        TDomainsInfo* domainsInfo = appData->DomainsInfo.Get();
        TTabletId rootHiveId = domainsInfo->GetHive();
        bool isRootHive = (rootHiveId == Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);

        Self->Nodes.clear();
        Self->TabletCategories.clear();
        Self->Tablets.clear();
        Self->OwnerToTablet.clear();
        Self->ObjectToTabletMetrics.clear();
        Self->TabletTypeToTabletMetrics.clear();
        Self->TabletTypeAllowedMetrics.clear();
        Self->StoragePools.clear();
        Self->Sequencer.Clear();
        Self->Keeper.Clear();
        Self->Domains.clear();
        Self->BlockedOwners.clear();

        Self->Domains[Self->RootDomainKey].Path = Self->RootDomainName;
        Self->Domains[Self->RootDomainKey].HiveId = rootHiveId;

        {
            // precharge
            auto tabletRowset = db.Table<Schema::Tablet>().Range().Select();
            auto tabletChannelRowset = db.Table<Schema::TabletChannel>().Range().Select();
            auto tabletChannelGenRowset = db.Table<Schema::TabletChannelGen>().Range().Select();
            auto metrics = db.Table<Schema::Metrics>().Range().Select();
            auto tabletFollowerGroupRowset = db.Table<Schema::TabletFollowerGroup>().Range().Select();
            auto tabletFollowerRowset = db.Table<Schema::TabletFollowerTablet>().Range().Select();
            auto tabletTypeAllowedMetrics = db.Table<Schema::TabletTypeMetrics>().Range().Select();
            auto stateRowset = db.Table<Schema::State>().Select();
            auto sequencesRowset = db.Table<Schema::Sequences>().Select();
            auto domainsRowset = db.Table<Schema::SubDomain>().Select();
            auto blockedOwnersRowset = db.Table<Schema::BlockedOwner>().Select();
            auto tabletOwnersRowset = db.Table<Schema::TabletOwners>().Select();
            auto nodeRowset = db.Table<Schema::Node>().Select();
            auto configRowset = db.Table<Schema::State>().Select();
            auto categoryRowset = db.Table<Schema::TabletCategory>().Select();
            auto availabilityRowset = db.Table<Schema::TabletAvailabilityRestrictions>().Select();
            auto operationsRowset = db.Table<Schema::OperationsLog>().Select();
            if (!tabletRowset.IsReady()
                    || !tabletChannelRowset.IsReady()
                    || !tabletChannelGenRowset.IsReady()
                    || !metrics.IsReady()
                    || !tabletFollowerGroupRowset.IsReady()
                    || !tabletFollowerRowset.IsReady()
                    || !tabletTypeAllowedMetrics.IsReady()
                    || !stateRowset.IsReady()
                    || !sequencesRowset.IsReady()
                    || !domainsRowset.IsReady()
                    || !blockedOwnersRowset.IsReady()
                    || !tabletOwnersRowset.IsReady()
                    || !nodeRowset.IsReady()
                    || !configRowset.IsReady()
                    || !categoryRowset.IsReady()
                    || !availabilityRowset.IsReady()
                    || !operationsRowset.IsReady())
                return false;
        }

        {
            auto configRowset = db.Table<Schema::State>().Key(TSchemeIds::State::DefaultState).Select();
            if (!configRowset.IsReady()) {
                return false;
            }
            if (!configRowset.EndOfSet()) {
                Self->DatabaseConfig = configRowset.GetValueOrDefault<Schema::State::Config>();
            }
        }

        {
            auto stateRowset = db.Table<Schema::State>().Select();
            if (!stateRowset.IsReady()) {
                return false;
            }
            while (!stateRowset.EndOfSet()) {
                if (stateRowset.HaveValue<Schema::State::Value>()) {
                    switch (stateRowset.GetKey()) {
                    case TSchemeIds::State::DatabaseVersion:
                        break;
                    case TSchemeIds::State::NextTabletId:
                        Self->NextTabletId = stateRowset.GetValueOrDefault<Schema::State::Value>(Self->NextTabletId);
                        break;
                    case TSchemeIds::State::MaxResourceCounter:
                        Self->DatabaseConfig.SetMaxResourceCounter(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxResourceCPU:
                        Self->DatabaseConfig.SetMaxResourceCPU(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxResourceMemory:
                        Self->DatabaseConfig.SetMaxResourceMemory(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxResourceNetwork:
                        Self->DatabaseConfig.SetMaxResourceNetwork(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxTabletsScheduled:
                        Self->DatabaseConfig.SetMaxTabletsScheduled(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxBootBatchSize:
                        Self->DatabaseConfig.SetMaxBootBatchSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::DrainInflight:
                        Self->DatabaseConfig.SetDrainInflight(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MinScatterToBalance:
                        Self->DatabaseConfig.SetMinScatterToBalance(double(stateRowset.GetValue<Schema::State::Value>()) / 100);
                        break;
                    case TSchemeIds::State::SpreadNeighbours:
                        Self->DatabaseConfig.SetSpreadNeighbours(stateRowset.GetValue<Schema::State::Value>() != 0);
                        break;
                    case TSchemeIds::State::DefaultUnitIOPS:
                        Self->DatabaseConfig.SetDefaultUnitIOPS(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::DefaultUnitThroughput:
                        Self->DatabaseConfig.SetDefaultUnitThroughput(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::DefaultUnitSize:
                        Self->DatabaseConfig.SetDefaultUnitSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::StorageOvercommit:
                        Self->DatabaseConfig.SetStorageOvercommit(double(stateRowset.GetValue<Schema::State::Value>()) / 100);
                        break;
                    case TSchemeIds::State::StorageBalanceStrategy:
                        Self->DatabaseConfig.SetStorageBalanceStrategy((NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy)stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::StorageSelectStrategy:
                        Self->DatabaseConfig.SetStorageSelectStrategy((NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy)stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::StorageSafeMode:
                        Self->DatabaseConfig.SetStorageSafeMode(stateRowset.GetValue<Schema::State::Value>() != 0);
                        break;
                    case TSchemeIds::State::RequestSequenceSize:
                        Self->DatabaseConfig.SetRequestSequenceSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MinRequestSequenceSize:
                        Self->DatabaseConfig.SetMinRequestSequenceSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxRequestSequenceSize:
                        Self->DatabaseConfig.SetMaxRequestSequenceSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MetricsWindowSize:
                        Self->DatabaseConfig.SetMetricsWindowSize(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::MaxNodeUsageToKick:
                        Self->DatabaseConfig.SetMaxNodeUsageToKick((double)stateRowset.GetValue<Schema::State::Value>() / 100);
                        break;
                    case TSchemeIds::State::ResourceChangeReactionPeriod:
                        Self->DatabaseConfig.SetResourceChangeReactionPeriod(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::TabletKickCooldownPeriod:
                        Self->DatabaseConfig.SetTabletKickCooldownPeriod(stateRowset.GetValue<Schema::State::Value>());
                        break;
                    case TSchemeIds::State::ResourceOvercommitment:
                        Self->DatabaseConfig.SetResourceOvercommitment((double)stateRowset.GetValue<Schema::State::Value>() / 100);
                        break;
                    case TSchemeIds::State::TabletOwnersSynced:
                        Self->TabletOwnersSynced = (bool)stateRowset.GetValue<Schema::State::Value>();
                        break;
                    }
                }
                stateRowset.Next();
            }
        }

        Self->BuildCurrentConfig();
        if (Self->CurrentConfig.HasWarmUpEnabled()) {
            Self->WarmUp = Self->CurrentConfig.GetWarmUpEnabled();
        } else {
            Self->WarmUp = Self->CurrentConfig.GetWarmUpEnabled() && !Self->AreWeRootHive();
        }

        Self->DefaultResourceMetricsAggregates.MaximumCPU.SetWindowSize(TDuration::MilliSeconds(Self->GetMetricsWindowSize()));
        Self->DefaultResourceMetricsAggregates.MaximumMemory.SetWindowSize(TDuration::MilliSeconds(Self->GetMetricsWindowSize()));
        Self->DefaultResourceMetricsAggregates.MaximumNetwork.SetWindowSize(TDuration::MilliSeconds(Self->GetMetricsWindowSize()));

        auto owners = db.Table<Schema::TabletOwners>().Select();
        if (!owners.IsReady()) {
            return false;
        }
        while (!owners.EndOfSet()) {
            auto begin = owners.GetValue<Schema::TabletOwners::Begin>();
            auto end = owners.GetValue<Schema::TabletOwners::End>();
            auto ownerId = owners.GetValue<Schema::TabletOwners::OwnerId>();

            Self->Keeper.AddOwnedSequence(ownerId, {begin, end});
            if (!owners.Next()) {
                return false;
            }
        }

        size_t numSequences = 0;
        auto sequences = db.Table<Schema::Sequences>().Select();
        if (!sequences.IsReady()) {
            return false;
        }
        while (!sequences.EndOfSet()) {
            auto begin = sequences.GetValue<Schema::Sequences::Begin>();
            auto end = sequences.GetValue<Schema::Sequences::End>();
            auto next = sequences.GetValue<Schema::Sequences::Next>();
            auto ownerId = sequences.GetValue<Schema::Sequences::OwnerId>();
            auto ownerIdx = sequences.GetValue<Schema::Sequences::OwnerIdx>();
            TSequencer::TSequence seq(begin, next, end);

            Self->Sequencer.AddSequence({ownerId, ownerIdx}, seq);
            ++numSequences;

            // remove after upgrade vvvv
            if (ownerId != TSequencer::NO_OWNER && Self->Keeper.GetOwner(seq.Begin) == TSequencer::NO_OWNER) {
                BLOG_W("THive::TTxLoadEverything fixing TabletOwners for " << seq << " to " << ownerId);
                Self->Keeper.AddOwnedSequence(ownerId, seq);
                db.Table<Schema::TabletOwners>().Key(seq.Begin, seq.End).Update<Schema::TabletOwners::OwnerId>(ownerId);
            }

            if (ownerId == TSequencer::NO_OWNER && !isRootHive && Self->Keeper.GetOwner(seq.Begin) == TSequencer::NO_OWNER) {
                BLOG_W("THive::TTxLoadEverything fixing TabletOwners for " << seq << " to " << Self->TabletID());
                Self->Keeper.AddOwnedSequence(Self->TabletID(), seq);
                db.Table<Schema::TabletOwners>().Key(seq.Begin, seq.End).Update<Schema::TabletOwners::OwnerId>(Self->TabletID());
            }
            // remove after upgrade ^^^^

            if (!sequences.Next()) {
                return false;
            }
        }

        BLOG_NOTICE("THive::TTxLoadEverything loaded " << numSequences << " sequences");

        auto tabletTypeAllowedMetrics = db.Table<Schema::TabletTypeMetrics>().Select();
        if (!tabletTypeAllowedMetrics.IsReady())
            return false;
        while (!tabletTypeAllowedMetrics.EndOfSet()) {
            auto type = tabletTypeAllowedMetrics.GetValue<Schema::TabletTypeMetrics::TabletType>();
            auto& allowedMetrics = Self->TabletTypeAllowedMetrics[type];
            allowedMetrics = tabletTypeAllowedMetrics.GetValue<Schema::TabletTypeMetrics::AllowedMetricIDs>();
            if (Find(allowedMetrics, NKikimrTabletBase::TMetrics::kCounterFieldNumber) == allowedMetrics.end()) {
                allowedMetrics.emplace_back(NKikimrTabletBase::TMetrics::kCounterFieldNumber);
            }
            if (!tabletTypeAllowedMetrics.Next())
                return false;
        }

        {
            size_t numSubDomains = 0;
            auto domainRowset = db.Table<Schema::SubDomain>().Range().Select();
            if (!domainRowset.IsReady())
                return false;
            while (!domainRowset.EndOfSet()) {
                ++numSubDomains;
                TSubDomainKey key(domainRowset.GetKey());
                TDomainInfo& domain = Self->Domains.emplace(key, TDomainInfo()).first->second;
                domain.Path = domainRowset.GetValueOrDefault<Schema::SubDomain::Path>();
                domain.HiveId = domainRowset.GetValueOrDefault<Schema::SubDomain::HiveId>();
                if (domain.HiveId == 0) {
                    domain.Path.clear(); // we will refresh domain one more time to see if it has Hive now
                }
                if (domainRowset.GetValueOrDefault<Schema::SubDomain::Primary>()) {
                    Self->PrimaryDomainKey = key;
                }
                if (domainRowset.HaveValue<Schema::SubDomain::ServerlessComputeResourcesMode>()) {
                    domain.ServerlessComputeResourcesMode = domainRowset.GetValue<Schema::SubDomain::ServerlessComputeResourcesMode>();
                }
                
                if (!domainRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numSubDomains << " subdomains");
        }

        {
            size_t numBlockedOwners = 0;
            auto blockedOwnerRowset = db.Table<Schema::BlockedOwner>().Range().Select();
            if (!blockedOwnerRowset.IsReady())
                return false;
            while (!blockedOwnerRowset.EndOfSet()) {
                ++numBlockedOwners;
                Self->BlockedOwners.emplace(blockedOwnerRowset.GetKey());
                if (!blockedOwnerRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numBlockedOwners << " blocked owners");
        }

        {
            size_t numNodes = 0;
            auto nodeRowset = db.Table<Schema::Node>().Range().Select();
            if (!nodeRowset.IsReady())
                return false;
            while (!nodeRowset.EndOfSet()) {
                ++numNodes;
                TNodeId nodeId = nodeRowset.GetValue<Schema::Node::ID>();
                TNodeInfo& node = Self->Nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, *Self)).first->second;
                node.Local = nodeRowset.GetValue<Schema::Node::Local>();
                node.Down = nodeRowset.GetValue<Schema::Node::Down>();
                node.Freeze = nodeRowset.GetValue<Schema::Node::Freeze>();
                node.Drain = nodeRowset.GetValueOrDefault<Schema::Node::Drain>();
                node.DrainInitiators = nodeRowset.GetValueOrDefault<Schema::Node::DrainInitiators>();
                node.ServicedDomains = nodeRowset.GetValueOrDefault<Schema::Node::ServicedDomains>();
                node.Statistics = nodeRowset.GetValueOrDefault<Schema::Node::Statistics>();
                node.Name = nodeRowset.GetValueOrDefault<Schema::Node::Name>();
                node.BecomeUpOnRestart = nodeRowset.GetValueOrDefault<Schema::Node::BecomeUpOnRestart>(false);
                if (node.BecomeUpOnRestart) {
                    // If a node must become up on restart, it must have been down
                    // That was not persisted to avoid issues with downgrades
                    node.Down = true;
                }
                if (nodeRowset.HaveValue<Schema::Node::Location>()) {
                    auto location = nodeRowset.GetValue<Schema::Node::Location>();
                    if (location.HasDataCenter()) {
                        node.Location = TNodeLocation(location);
                        node.LocationAcquired = true;
                    }
                }
                if (!node.ServicedDomains) {
                    node.ServicedDomains = { Self->RootDomainKey };
                }
                node.LastSeenServicedDomains = node.ServicedDomains; // to keep Down and Freeze flags on restarts
                if (!(bool)node.Local) {
                    // it's safe to call here, because there is no any tablets in the node yet
                    node.BecomeDisconnected();
                }
                if (Self->TryToDeleteNode(&node)) {
                    // node is deleted from hashmap
                    db.Table<Schema::Node>().Key(nodeId).Delete();
                } else if (node.IsUnknown() && node.LocationAcquired) {
                    Self->AddRegisteredDataCentersNode(node.Location.GetDataCenterId(), node.Id);
                }
                if (!nodeRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numNodes << " nodes");
        }

        {
            size_t numTabletCategories = 0;
            auto categoryRowset = db.Table<Schema::TabletCategory>().Range().Select();
            if (!categoryRowset.IsReady())
                return false;
            while (!categoryRowset.EndOfSet()) {
                ++numTabletCategories;
                TTabletCategoryId categoryId = categoryRowset.GetValue<Schema::TabletCategory::ID>();
                TTabletCategoryInfo& category = Self->TabletCategories.insert(std::make_pair(categoryId, TTabletCategoryInfo(categoryId))).first->second;
                category.MaxDisconnectTimeout = categoryRowset.GetValueOrDefault<Schema::TabletCategory::MaxDisconnectTimeout>();
                category.StickTogetherInDC = categoryRowset.GetValueOrDefault<Schema::TabletCategory::StickTogetherInDC>();
                if (!categoryRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTabletCategories << " tablet categories");
        }

        if (auto systemCategoryId = Self->CurrentConfig.GetSystemTabletCategoryId(); systemCategoryId != 0 && Self->TabletCategories.empty()) {
            db.Table<Schema::TabletCategory>().Key(systemCategoryId).Update<Schema::TabletCategory::StickTogetherInDC>(true);
            TTabletCategoryInfo& systemCategory = Self->TabletCategories.emplace(systemCategoryId, systemCategoryId).first->second;
            systemCategory.StickTogetherInDC = true;
        }

        TTabletId maxTabletId = 0;

        {
            size_t numTablets = 0;
            auto tabletRowset = db.Table<Schema::Tablet>().Range().Select();
            if (!tabletRowset.IsReady())
                return false;
            while (!tabletRowset.EndOfSet()) {
                ++numTablets;
                TTabletId tabletId = tabletRowset.GetValue<Schema::Tablet::ID>();
                maxTabletId = std::max(maxTabletId, UniqPartFromTabletID(tabletId));
                TLeaderTabletInfo& tablet = Self->Tablets.emplace(
                            std::piecewise_construct,
                            std::tuple<TTabletId>(tabletId),
                            std::tuple<TTabletId, THive&>(tabletId, *Self)).first->second;
                tablet.State = tabletRowset.GetValue<Schema::Tablet::State>();
                tablet.SetType(tabletRowset.GetValue<Schema::Tablet::TabletType>());

                TObjectId objectId = tabletRowset.GetValueOrDefault<Schema::Tablet::ObjectID>();
                TOwnerIdxType::TValueType owner = tabletRowset.GetValue<Schema::Tablet::Owner>();
                Self->OwnerToTablet.emplace(owner, tabletId);
                tablet.Owner = owner;
                tablet.ObjectId = {owner.first, objectId};

                Self->ObjectToTabletMetrics[tablet.ObjectId].IncreaseCount();
                Self->TabletTypeToTabletMetrics[tablet.Type].IncreaseCount();
                tablet.NodeFilter.AllowedNodes = tabletRowset.GetValue<Schema::Tablet::AllowedNodes>();
                if (tabletRowset.HaveValue<Schema::Tablet::AllowedDataCenters>()) {
                    // this is priority format due to migration issues; when migration is complete, this code will
                    // be removed
                    for (const ui32 dcId : tabletRowset.GetValue<Schema::Tablet::AllowedDataCenters>()) {
                        tablet.NodeFilter.AllowedDataCenters.push_back(DataCenterToString(dcId));
                    }
                } else {
                    tablet.NodeFilter.AllowedDataCenters = tabletRowset.GetValueOrDefault<Schema::Tablet::AllowedDataCenterIds>();
                }
                tablet.DataCentersPreference = tabletRowset.GetValueOrDefault<Schema::Tablet::DataCentersPreference>();
                TVector<TSubDomainKey> allowedDomains = tabletRowset.GetValueOrDefault<Schema::Tablet::AllowedDomains>();
                TSubDomainKey objectDomain = TSubDomainKey(tabletRowset.GetValueOrDefault<Schema::Tablet::ObjectDomain>());
                tablet.AssignDomains(objectDomain, allowedDomains);
                //tablet.Weight = tabletRowset.GetValueOrDefault<Schema::Tablet::Weight>(1000);
                tablet.NodeId = tabletRowset.GetValue<Schema::Tablet::LeaderNode>();
                tablet.KnownGeneration = tabletRowset.GetValue<Schema::Tablet::KnownGeneration>();
                tablet.ActorsToNotify = tabletRowset.GetValueOrDefault<Schema::Tablet::ActorsToNotify>();
                if (tabletRowset.HaveValue<Schema::Tablet::ActorToNotify>()) {
                    tablet.ActorsToNotify.push_back(tabletRowset.GetValue<Schema::Tablet::ActorToNotify>());
                    tablet.ActorsToNotify.erase(
                        std::unique(tablet.ActorsToNotify.begin(), tablet.ActorsToNotify.end()),
                        tablet.ActorsToNotify.end()
                    );
                }
                TTabletCategoryId categoryId = 0;
                if (tabletRowset.HaveValue<Schema::Tablet::Category>()) {
                    categoryId = tabletRowset.GetValue<Schema::Tablet::Category>();
                }
                if (categoryId == 0 && Self->IsSystemTablet(tablet.Type)) {
                    categoryId = Self->CurrentConfig.GetSystemTabletCategoryId();
                }
                if (categoryId != 0) {
                    tablet.Category = &Self->GetTabletCategory(categoryId);
                    tablet.Category->Tablets.insert(&tablet);
                }
                tablet.BootMode = tabletRowset.GetValue<Schema::Tablet::BootMode>();
                tablet.LockedToActor = tabletRowset.GetValueOrDefault<Schema::Tablet::LockedToActor>();
                tablet.LockedReconnectTimeout = TDuration::MilliSeconds(tabletRowset.GetValueOrDefault<Schema::Tablet::LockedReconnectTimeout>());
                if (tablet.LockedToActor) {
                    TNodeId nodeId = tablet.LockedToActor.NodeId();
                    auto it = Self->Nodes.find(nodeId);
                    if (it == Self->Nodes.end()) {
                        // Tablet was locked to a node that had no local service
                        it = Self->Nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, *Self)).first;
                    }
                    it->second.LockedTablets.insert(&tablet);
                }

                tablet.SeizedByChild = tabletRowset.GetValueOrDefault<Schema::Tablet::SeizedByChild>();
                tablet.NeedToReleaseFromParent = tabletRowset.GetValueOrDefault<Schema::Tablet::NeedToReleaseFromParent>();
                tablet.ChannelProfileReassignReason = tabletRowset.GetValueOrDefault<Schema::Tablet::ReassignReason>();
                tablet.Statistics = tabletRowset.GetValueOrDefault<Schema::Tablet::Statistics>();

                if (tablet.NodeId == 0) {
                    tablet.BecomeStopped();
                } else {
                    auto it = Self->Nodes.find(tablet.NodeId);
                    if (it != Self->Nodes.end() && it->second.IsUnknown()) {
                        tablet.BecomeUnknown(&it->second);
                    } else {
                        tablet.NodeId = 0;
                        tablet.BecomeStopped();
                    }
                }
                tablet.InitTabletMetrics();

                tablet.TabletStorageInfo.Reset(new TTabletStorageInfo(tabletId, tablet.Type));
                tablet.TabletStorageInfo->Version = tabletRowset.GetValueOrDefault<Schema::Tablet::TabletStorageVersion>();
                tablet.TabletStorageInfo->TenantPathId = tablet.GetTenant();

                if (!tabletRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTablets << " tablets");
        }

        {
            size_t numTabletChannels = 0;
            size_t numMissingTablets = 0;
            auto tabletChannelRowset = db.Table<Schema::TabletChannel>().Select();
            if (!tabletChannelRowset.IsReady())
                return false;

            while (!tabletChannelRowset.EndOfSet()) {
                ++numTabletChannels;
                TTabletId tabletId = tabletChannelRowset.GetValue<Schema::TabletChannel::Tablet>();
                TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
                if (tablet) {
                    ui32 channelId = tabletChannelRowset.GetValue<Schema::TabletChannel::Channel>();
                    TString storagePool = tabletChannelRowset.GetValue<Schema::TabletChannel::StoragePool>();
                    Y_ABORT_UNLESS(tablet->BoundChannels.size() == channelId);
                    tablet->BoundChannels.emplace_back();
                    NKikimrStoragePool::TChannelBind& bind = tablet->BoundChannels.back();
                    if (tabletChannelRowset.HaveValue<Schema::TabletChannel::Binding>()) {
                        bind = tabletChannelRowset.GetValue<Schema::TabletChannel::Binding>();
                    }
                    bind.SetStoragePoolName(storagePool);
                    Self->InitDefaultChannelBind(bind);
                    tablet->TabletStorageInfo->Channels.emplace_back(channelId, storagePool);

                    if (tabletChannelRowset.GetValue<Schema::TabletChannel::NeedNewGroup>()) {
                        tablet->ChannelProfileNewGroup.set(channelId);
                    }
                } else {
                    ++numMissingTablets;
                }
                if (!tabletChannelRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTabletChannels << " tablet/channel pairs ("
                    << numMissingTablets << " for missing tablets)");
        }

        {
            size_t numTabletChannelHistories = 0;
            size_t numMissingTablets = 0;
            auto tabletChannelGenRowset = db.Table<Schema::TabletChannelGen>().Select();
            if (!tabletChannelGenRowset.IsReady())
                return false;

            while (!tabletChannelGenRowset.EndOfSet()) {
                ++numTabletChannelHistories;
                TTabletId tabletId = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Tablet>();
                TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
                if (tablet) {
                    ui32 channelId = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Channel>();
                    ui32 generationId = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Generation>();
                    ui32 groupId = tabletChannelGenRowset.GetValue<Schema::TabletChannelGen::Group>();
                    TInstant timestamp = TInstant::MilliSeconds(tabletChannelGenRowset.GetValueOrDefault<Schema::TabletChannelGen::Timestamp>());
                    while (tablet->TabletStorageInfo->Channels.size() <= channelId) {
                        tablet->TabletStorageInfo->Channels.emplace_back();
                        tablet->TabletStorageInfo->Channels.back().Channel = tablet->TabletStorageInfo->Channels.size() - 1;
                    }
                    TTabletChannelInfo::THistoryEntry entry(generationId, groupId, timestamp);
                    auto deletedAtGeneration = tabletChannelGenRowset.GetValueOrDefault<Schema::TabletChannelGen::DeletedAtGeneration>();
                    if (deletedAtGeneration) {
                        tablet->DeletedHistory.emplace(channelId, entry, deletedAtGeneration);
                    } else {
                        TTabletChannelInfo& channel = tablet->TabletStorageInfo->Channels[channelId];
                        channel.History.push_back(entry);
                    }
                } else {
                    ++numMissingTablets;
                }
                if (!tabletChannelGenRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTabletChannelHistories << " tablet/channel history items ("
                    << numMissingTablets << " for missing tablets)");
        }

        for (auto& [tabletId, tabletInfo] : Self->Tablets) {
            tabletInfo.AcquireAllocationUnits();
        }
        BLOG_NOTICE("THive::TTxLoadEverything initialized allocation units for " << Self->Tablets.size() << " tablets");

        {
            size_t numTabletFollowerGroups = 0;
            size_t numMissingTablets = 0;
            auto tabletFollowerGroupRowset = db.Table<Schema::TabletFollowerGroup>().Select();
            if (!tabletFollowerGroupRowset.IsReady())
                return false;
            while (!tabletFollowerGroupRowset.EndOfSet()) {
                ++numTabletFollowerGroups;
                TTabletId tabletId = tabletFollowerGroupRowset.GetValue<Schema::TabletFollowerGroup::TabletID>();
                TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
                if (tablet) {
                    TFollowerGroup& followerGroup = tablet->AddFollowerGroup();
                    followerGroup.Id = tabletFollowerGroupRowset.GetValue<Schema::TabletFollowerGroup::GroupID>();
                    followerGroup.SetFollowerCount(tabletFollowerGroupRowset.GetValue<Schema::TabletFollowerGroup::FollowerCount>());
                    followerGroup.AllowLeaderPromotion = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowLeaderPromotion>();
                    followerGroup.AllowClientRead = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowClientRead>();
                    followerGroup.NodeFilter.AllowedNodes = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowedNodes>();

                    if (tabletFollowerGroupRowset.HaveValue<Schema::TabletFollowerGroup::AllowedDataCenters>()) {
                        // this is priority format due to migration issues; when migration is complete, this code will
                        // be removed
                        for (const ui32 dcId : tabletFollowerGroupRowset.GetValue<Schema::TabletFollowerGroup::AllowedDataCenters>()) {
                            followerGroup.NodeFilter.AllowedDataCenters.push_back(DataCenterToString(dcId));
                        }
                    } else {
                        followerGroup.NodeFilter.AllowedDataCenters = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::AllowedDataCenterIds>();
                    }

                    followerGroup.RequireAllDataCenters = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::RequireAllDataCenters>();
                    followerGroup.LocalNodeOnly = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::LocalNodeOnly>();
                    followerGroup.FollowerCountPerDataCenter = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>();
                    followerGroup.RequireDifferentNodes = tabletFollowerGroupRowset.GetValueOrDefault<Schema::TabletFollowerGroup::RequireDifferentNodes>();
                } else {
                    ++numMissingTablets;
                }
                if (!tabletFollowerGroupRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTabletFollowerGroups << " tablet follower groups ("
                    << numMissingTablets << " for missing tablets)");
        }

        {
            size_t numTabletFollowers = 0;
            size_t numMissingTablets = 0;
            auto tabletFollowerRowset = db.Table<Schema::TabletFollowerTablet>().Select();
            if (!tabletFollowerRowset.IsReady())
                return false;
            while (!tabletFollowerRowset.EndOfSet()) {
                ++numTabletFollowers;
                TTabletId tabletId = tabletFollowerRowset.GetValue<Schema::TabletFollowerTablet::TabletID>();
                TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
                if (tablet) {
                    TFollowerGroupId followerGroupId = tabletFollowerRowset.GetValue<Schema::TabletFollowerTablet::GroupID>();
                    TFollowerId followerId = tabletFollowerRowset.GetValue<Schema::TabletFollowerTablet::FollowerID>();
                    TNodeId nodeId = tabletFollowerRowset.GetValue<Schema::TabletFollowerTablet::FollowerNode>();
                    TFollowerGroup& followerGroup = tablet->GetFollowerGroup(followerGroupId);
                    TFollowerTabletInfo& follower = tablet->AddFollower(followerGroup, followerId);
                    follower.Statistics = tabletFollowerRowset.GetValueOrDefault<Schema::TabletFollowerTablet::Statistics>();
                    if (tabletFollowerRowset.HaveValue<Schema::TabletFollowerTablet::DataCenter>()) {
                        auto dc = tabletFollowerRowset.GetValue<Schema::TabletFollowerTablet::DataCenter>();
                        follower.NodeFilter.AllowedDataCenters = {dc};
                        Self->DataCenters[dc].Followers[{tabletId, followerGroup.Id}].push_back(std::prev(tablet->Followers.end()));
                    }
                    if (nodeId == 0) {
                        follower.BecomeStopped();
                    } else {
                        auto it = Self->Nodes.find(nodeId);
                        if (it != Self->Nodes.end() && it->second.IsUnknown()) {
                            follower.BecomeUnknown(&it->second);
                        } else {
                            follower.BecomeStopped();
                        }
                    }
                    follower.InitTabletMetrics();
                } else {
                    ++numMissingTablets;
                }
                if (!tabletFollowerRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numTabletFollowers << " tablet followers ("
                    << numMissingTablets << " for missing tablets)");
        }

        // Compatability: some per-dc followers do not have their datacenter set - try to set it now
        for (auto& [tabletId, tablet] : Self->Tablets) {
            for (auto& group : tablet.FollowerGroups) {
                if (!group.FollowerCountPerDataCenter) {
                    continue;
                }
                std::map<TDataCenterId, i32> dataCentersToCover; // dc -> need x more followers in dc
                for (const auto& [dc, _] : Self->DataCenters) {
                    dataCentersToCover[dc] = group.GetFollowerCountForDataCenter(dc);
                }
                auto groupId = group.Id;
                auto filterGroup = [groupId](auto&& follower) { return follower->FollowerGroup.Id == groupId;};
                auto groupFollowersIters = std::views::iota(tablet.Followers.begin(), tablet.Followers.end()) | std::views::filter(filterGroup);
                std::vector<TDataCenterInfo::TFollowerIter> followersWithoutDc;
                for (auto followerIt : groupFollowersIters) {
                    auto& allowedDc = followerIt->NodeFilter.AllowedDataCenters;
                    if (allowedDc.size() == 1) {
                        --dataCentersToCover[allowedDc.front()];
                        continue;
                    }
                    bool ok = false;
                    if (followerIt->Node) {
                        auto dc = followerIt->Node->Location.GetDataCenterId();
                        auto& cnt = dataCentersToCover[dc];
                        if (cnt > 0) {
                            --cnt;
                            allowedDc = {dc};
                            Self->DataCenters[dc].Followers[{tabletId, groupId}].push_back(followerIt);
                            db.Table<Schema::TabletFollowerTablet>().Key(tabletId, followerIt->Id).Update<Schema::TabletFollowerTablet::DataCenter>(dc);
                            ok = true;
                        }
                    }
                    if (!ok) {
                        followersWithoutDc.push_back(followerIt);
                    }
                }
                auto dcIt = dataCentersToCover.begin();
                for (auto follower : followersWithoutDc) {
                    while (dcIt != dataCentersToCover.end() && dcIt->second <= 0) {
                        ++dcIt;
                    }
                    if (dcIt == dataCentersToCover.end()) {
                        break;
                    }
                    follower->NodeFilter.AllowedDataCenters = {dcIt->first};
                    Self->DataCenters[dcIt->first].Followers[{tabletId, groupId}].push_back(follower);
                    db.Table<Schema::TabletFollowerTablet>().Key(follower->GetFullTabletId()).Update<Schema::TabletFollowerTablet::DataCenter>(dcIt->first);
                    --dcIt->second;
                }
            }
        }

        {
            size_t numMetrics = 0;
            size_t numMissingTablets = 0;
            auto metricsRowset = db.Table<Schema::Metrics>().Select();
            if (!metricsRowset.IsReady())
                return false;
            while (!metricsRowset.EndOfSet()) {
                ++numMetrics;
                TTabletId tabletId = metricsRowset.GetValue<Schema::Metrics::TabletID>();
                TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(tabletId);
                if (tablet) {
                    TFollowerId followerId = metricsRowset.GetValue<Schema::Metrics::FollowerID>();
                    auto* leaderOrFollower = tablet->FindTablet(followerId);
                    if (leaderOrFollower) {
                        leaderOrFollower->MutableResourceMetricsAggregates().MaximumCPU.InitiaizeFrom(metricsRowset.GetValueOrDefault<Schema::Metrics::MaximumCPU>());
                        leaderOrFollower->MutableResourceMetricsAggregates().MaximumMemory.InitiaizeFrom(metricsRowset.GetValueOrDefault<Schema::Metrics::MaximumMemory>());
                        leaderOrFollower->MutableResourceMetricsAggregates().MaximumNetwork.InitiaizeFrom(metricsRowset.GetValueOrDefault<Schema::Metrics::MaximumNetwork>());
                        // do not reorder
                        leaderOrFollower->UpdateResourceUsage(metricsRowset.GetValueOrDefault<Schema::Metrics::ProtoMetrics>());
                    }
                } else {
                    ++numMissingTablets;
                }
                if (!metricsRowset.Next())
                    return false;
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numMetrics << " metrics ("
                    << numMissingTablets << " for missing tablets)");
        }

        {
            size_t numRestrictions = 0;
            size_t numMissingNodes = 0;
            auto availabilityRestrictionsRowset = db.Table<Schema::TabletAvailabilityRestrictions>().Select();
            if (!availabilityRestrictionsRowset.IsReady()) {
                return false;
            }
            while (!availabilityRestrictionsRowset.EndOfSet()) {
                ++numRestrictions;
                TNodeId nodeId = availabilityRestrictionsRowset.GetValue<Schema::TabletAvailabilityRestrictions::Node>();
                auto node = Self->FindNode(nodeId);
                if (node) {
                    auto tabletType = availabilityRestrictionsRowset.GetValue<Schema::TabletAvailabilityRestrictions::TabletType>();
                    auto maxCount = availabilityRestrictionsRowset.GetValue<Schema::TabletAvailabilityRestrictions::MaxCount>();
                    node->TabletAvailabilityRestrictions[tabletType] = maxCount;
                } else {
                    ++numMissingNodes;
                }
                if (!availabilityRestrictionsRowset.Next()) {
                    return false;
                }
            }
            BLOG_NOTICE("THive::TTxLoadEverything loaded " << numRestrictions << " tablet availability restrictions ("
                        << numMissingNodes << " for missing nodes)");
        }

        size_t numDeletedNodes = 0;
        size_t numDeletedRestrictions = 0;
        for (auto itNode = Self->Nodes.begin(); itNode != Self->Nodes.end();) {
            if (itNode->second.CanBeDeleted()) {
                ++numDeletedNodes;
                auto restrictionsRowset = db.Table<Schema::TabletAvailabilityRestrictions>().Range(itNode->first).Select();
                while (!restrictionsRowset.EndOfSet()) {
                    ++numDeletedRestrictions;
                    db.Table<Schema::TabletAvailabilityRestrictions>().Key(restrictionsRowset.GetKey()).Delete();
                    if (!restrictionsRowset.Next()) {
                        return false;
                    }
                }
                db.Table<Schema::Node>().Key(itNode->first).Delete();
                itNode = Self->Nodes.erase(itNode);
            } else {
                ++itNode;
            }
        }
        BLOG_NOTICE("THive::TTxLoadEverything deleted " << numDeletedNodes << " unnecessary nodes << (and " << numDeletedRestrictions << " restrictions for them)");

        TTabletId nextTabletId = Max(maxTabletId + 1, Self->NextTabletId);

        auto operationsRowset = db.Table<Schema::OperationsLog>().All().Reverse().Select();
        if (!operationsRowset.IsReady()) {
            return false;
        }
        if (operationsRowset.IsValid()) {
            Self->OperationsLogIndex = operationsRowset.GetValue<Schema::OperationsLog::Index>();
        }

        if (isRootHive) {
            if (numSequences == 0) {
                ui64 freeSequenceIdx = 0;
                BLOG_D("THive::TTxLoadEverything Self->NextTabletId = " << Self->NextTabletId << " NextTabletId = " << nextTabletId);
                if (nextTabletId < TABLET_ID_BLACKHOLE_BEGIN) {
                    TSequencer::TOwnerType owner(TSequencer::NO_OWNER, freeSequenceIdx++);
                    TSequencer::TSequence sequence({0x10000, std::max<TTabletId>(nextTabletId, 0x10000), TABLET_ID_BLACKHOLE_BEGIN});
                    Self->Sequencer.AddFreeSequence(owner, sequence);
                    db.Table<Schema::Sequences>().Key(owner)
                            .Update<Schema::Sequences::Begin, Schema::Sequences::Next, Schema::Sequences::End>(
                                sequence.Begin, sequence.Next, sequence.End);
                }
                {
                    TSequencer::TOwnerType owner(TSequencer::NO_OWNER, freeSequenceIdx++);
                    TSequencer::TSequence sequence({TABLET_ID_BLACKHOLE_END, std::max<TTabletId>(nextTabletId, TABLET_ID_BLACKHOLE_END), 0xFFFFFFFFFFF});
                    Self->Sequencer.AddFreeSequence(owner, sequence);
                    db.Table<Schema::Sequences>().Key(owner)
                            .Update<Schema::Sequences::Begin, Schema::Sequences::Next, Schema::Sequences::End>(
                                sequence.Begin, sequence.Next, sequence.End);
                }
            }
        }

        if (numSequences != 0) {
            std::vector<TSequencer::TOwnerType> modified;
            BLOG_D("THive::TTxLoadEverything NextElement = " << Self->Sequencer.GetNextElement() << " NextTabletId = " << nextTabletId);
            while (Self->Sequencer.GetNextElement() < nextTabletId) {
                TSequencer::TElementType element = Self->Sequencer.AllocateElement(modified);
                SortUnique(modified);
                if (element == TSequencer::NO_ELEMENT) {
                    BLOG_ERROR("THive::TTxLoadEverything - unable to equalize NextTabletId " << nextTabletId << " - could not allocate free element");
                    break;
                }
            }
            if (!modified.empty()) {
                for (auto owner : modified) {
                    auto sequence = Self->Sequencer.GetSequence(owner);
                    BLOG_CRIT("THive::TTxLoadEverything - equalizing sequence " << owner << " to " << sequence);
                    db.Table<Schema::Sequences>()
                            .Key(owner)
                            .Update<Schema::Sequences::Begin, Schema::Sequences::Next, Schema::Sequences::End>(sequence.Begin, sequence.Next, sequence.End);
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        BLOG_NOTICE("THive::TTxLoadEverything::Complete " << Self->DatabaseConfig.ShortDebugString());
        ui64 tabletsTotal = 0;
        for (auto it = Self->Tablets.begin(); it != Self->Tablets.end(); ++it) {
            ++tabletsTotal;
            for (const TTabletInfo& follower : it->second.Followers) {
                ++tabletsTotal;
                if (follower.IsLeader()) {
                    follower.AsLeader();
                } else {
                    follower.AsFollower();
                }
            }
        }

        Self->Become(&TSelf::StateWork);
        Self->SetCounterTabletsTotal(tabletsTotal);
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_FREE].Set(Self->Sequencer.FreeSize());
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_ALLOCATED].Set(Self->Sequencer.AllocatedSequencesSize());
        Self->ExpectedNodes = Self->Nodes.size();
        Self->TabletCounters->Simple()[NHive::COUNTER_NODES_TOTAL].Set(Self->ExpectedNodes);
        Self->MigrationState = NKikimrHive::EMigrationState::MIGRATION_READY;
        ctx.Send(Self->SelfId(), new TEvPrivate::TEvBootTablets());

        for (auto it = Self->Nodes.begin(); it != Self->Nodes.end(); ++it) {
            Self->ScheduleUnlockTabletExecution(it->second);
        }
    }
};

ITransaction* THive::CreateLoadEverything() {
    return new TTxLoadEverything(this);
}

} // NHive
} // NKikimr
