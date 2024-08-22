#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxCreateTablet : public TTransactionBase<THive> {
    NKikimrHive::TEvCreateTablet RequestData;
    const ui64 OwnerId;
    const ui64 OwnerIdx;
    const TTabletTypes::EType TabletType;

    const TActorId Sender;
    const ui64 Cookie;

    NKikimrHive::EErrorReason ErrorReason;
    ui64 TabletId;
    TObjectId ObjectId;
    TSubDomainKey ObjectDomain;

    TChannelsBindings BoundChannels;
    TVector<TNodeId> AllowedNodeIds;
    TVector<TDataCenterId> AllowedDataCenterIds;
    NKikimrHive::TDataCentersPreference DataCentersPreference;
    TVector<TSubDomainKey> AllowedDomains;
    NKikimrHive::TTabletCategory TabletCategory;
    TVector<NKikimrHive::TFollowerGroup> FollowerGroups;
    NKikimrHive::ETabletBootMode BootMode;
    NKikimrHive::TForwardRequest ForwardRequest;

    NKikimrHive::EBalancerPolicy BalancerPolicy;

    TSideEffects SideEffects;

public:
    TTxCreateTablet(NKikimrHive::TEvCreateTablet record, const TActorId& sender, const ui64 cookie, THive* hive)
        : TBase(hive)
        , RequestData(std::move(record))
        , OwnerId(RequestData.GetOwner())
        , OwnerIdx(RequestData.GetOwnerIdx())
        , TabletType((TTabletTypes::EType)RequestData.GetTabletType())
        , Sender(sender)
        , Cookie(cookie)
        , TabletId(0)
        , ObjectId(0)
        , ObjectDomain(RequestData.GetObjectDomain())
        , BoundChannels(RequestData.GetBindedChannels().begin(), RequestData.GetBindedChannels().end())
        , AllowedDomains(RequestData.GetAllowedDomains().begin(), RequestData.GetAllowedDomains().end())
        , BootMode(RequestData.GetTabletBootMode())
        , BalancerPolicy(RequestData.GetBalancerPolicy())
    {
        const ui32 allowedNodeIdsSize = RequestData.AllowedNodeIDsSize();
        AllowedNodeIds.reserve(allowedNodeIdsSize);
        for (ui32 idx = 0; idx < allowedNodeIdsSize; ++idx) {
            AllowedNodeIds.push_back(RequestData.GetAllowedNodeIDs(idx));
        }
        Sort(AllowedNodeIds);

        if (const auto& x = RequestData.GetAllowedDataCenters(); !x.empty()) {
            AllowedDataCenterIds.insert(AllowedDataCenterIds.end(), x.begin(), x.end());
        } else {
            for (const auto& dataCenterId : RequestData.GetAllowedDataCenterNumIDs()) {
                AllowedDataCenterIds.push_back(DataCenterToString(dataCenterId));
            }
        }
        Sort(AllowedDataCenterIds);

        DataCentersPreference = RequestData.GetDataCentersPreference();
        if (RequestData.HasTabletCategory()) {
            TabletCategory.CopyFrom(RequestData.GetTabletCategory());
        }
        auto& followerGroups = RequestData.GetFollowerGroups();
        std::copy(followerGroups.begin(), followerGroups.end(), std::back_inserter(FollowerGroups));
        if (FollowerGroups.empty() &&
                (RequestData.HasFollowerCount()
                 || RequestData.HasAllowFollowerPromotion()
                 || RequestData.HasCrossDataCenterFollowers()
                 || RequestData.HasCrossDataCenterFollowerCount())) {
            FollowerGroups.emplace_back();
            NKikimrHive::TFollowerGroup& compatibilityGroup(FollowerGroups.back());
            if (RequestData.HasAllowFollowerPromotion()) {
                compatibilityGroup.SetAllowLeaderPromotion(RequestData.GetAllowFollowerPromotion());
            }
            if (RequestData.HasCrossDataCenterFollowers()) {
                compatibilityGroup.SetFollowerCount(1);
                compatibilityGroup.SetFollowerCountPerDataCenter(true);
                compatibilityGroup.SetRequireAllDataCenters(true);
            }
            if (RequestData.HasCrossDataCenterFollowerCount()) {
                compatibilityGroup.SetFollowerCount(RequestData.GetCrossDataCenterFollowerCount());
                compatibilityGroup.SetFollowerCountPerDataCenter(true);
                compatibilityGroup.SetRequireAllDataCenters(true);
            }
            if (RequestData.HasFollowerCount()) {
                compatibilityGroup.SetFollowerCount(RequestData.GetFollowerCount());
            }
            compatibilityGroup.SetAllowClientRead(true);
        }
        ObjectId = RequestData.GetObjectId();
    }

    bool UpdateChannelsBinding(TLeaderTabletInfo& tablet, NIceDb::TNiceDb& db) {
        Y_ABORT_UNLESS(tablet.BoundChannels.size() <= BoundChannels.size(), "only expansion channels number is allowed in Binded Channels");

        std::bitset<MAX_TABLET_CHANNELS> newChannels;

        // compare channel list with erasure and category information
        for (ui32 channelId = 0; channelId < tablet.BoundChannels.size(); ++channelId) {
            auto& channelA = tablet.BoundChannels[channelId];
            auto channelB = BoundChannels[channelId]; // copy, not reference
            Self->InitDefaultChannelBind(channelB);
            if (channelA.SerializeAsString() != channelB.SerializeAsString()) {
                db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::StoragePool>(BoundChannels[channelId].GetStoragePoolName());
                db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::Binding>(BoundChannels[channelId]);
                db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::NeedNewGroup>(true);
                newChannels.set(channelId);
                tablet.ReleaseAllocationUnit(channelId);
            }
        }

        // new channels found in the tablet profile
        for (ui32 channelId = tablet.BoundChannels.size(); channelId < BoundChannels.size(); ++channelId) {
            auto channel = BoundChannels[channelId]; // copy, not reference
            Self->InitDefaultChannelBind(channel);
            db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::StoragePool>(BoundChannels[channelId].GetStoragePoolName());
            db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::Binding>(BoundChannels[channelId]);
            db.Table<Schema::TabletChannel>().Key(TabletId, channelId).Update<Schema::TabletChannel::NeedNewGroup>(true);
            newChannels.set(channelId);
        }

        if (newChannels.any()) {
            tablet.ChannelProfileNewGroup |= newChannels;
            tablet.State = ETabletState::GroupAssignment;
            tablet.ChannelProfileReassignReason = NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO;
            tablet.BoundChannels = BoundChannels;
            for (auto& bind : tablet.BoundChannels) {
                Self->InitDefaultChannelBind(bind);
            }
            db.Table<Schema::Tablet>().Key(TabletId)
                .Update<Schema::Tablet::State, Schema::Tablet::ReassignReason, Schema::Tablet::ActorsToNotify>(
                    tablet.State, NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO, {Sender}
                    );
            return true;
        }
        return false;
    }

    bool ValidateChannelsBinding(TLeaderTabletInfo& tablet) {
        if (BoundChannels.size() < tablet.BoundChannels.size()) {
            ErrorReason = NKikimrHive::ERROR_REASON_CHANNELS_CANNOT_SHRINK;
            return false;
        }
        return true;
    }

    void PostponeCreateTablet() {
        THive::TPendingCreateTablet& pendingCreateTablet(Self->PendingCreateTablets[{OwnerId, OwnerIdx}]);
        pendingCreateTablet.CreateTablet = RequestData; // TODO: consider std::move
        pendingCreateTablet.Sender = Sender;
        pendingCreateTablet.Cookie = Cookie;
    }

    void RequestFreeSequence() {
        if (Self->AreWeSubDomainHive()) {
            if (!Self->RequestingSequenceNow) {
                Self->RequestFreeSequence();
            }
        }
    }

    void ReplyToSender(NKikimrProto::EReplyStatus status) {
        BLOG_D("THive::TTxCreateTablet::Execute TabletId: " << TabletId <<
            " Status: " << NKikimrProto::EReplyStatus_Name(status));
        Y_ABORT_UNLESS(!!Sender);
        THolder<TEvHive::TEvCreateTabletReply> reply = MakeHolder<TEvHive::TEvCreateTabletReply>(status, OwnerId, OwnerIdx, TabletId, Self->TabletID(), ErrorReason);
        if (ForwardRequest.HasHiveTabletId()) {
            reply->Record.MutableForwardRequest()->CopyFrom(ForwardRequest);
        }
        SideEffects.Send(Sender, reply.Release(), 0, Cookie);
    }

    void ProcessTablet(TLeaderTabletInfo& tablet) {
        if (tablet.IsReadyToAssignGroups()) {
            tablet.InitiateAssignTabletGroups();
        } else if (tablet.IsBootingSuppressed()) {
            // Tablet will never boot, so notify about creation right now
            for (const TActorId& actor : tablet.ActorsToNotify) {
                SideEffects.Send(actor, new TEvHive::TEvTabletCreationResult(NKikimrProto::OK, TabletId));
            }
            tablet.ActorsToNotify.clear();
        } else {
            tablet.TryToBoot();
        }
    }

    TTxType GetTxType() const override { return NHive::TXTYPE_CREATE_TABLET; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        const TOwnerIdxType::TValueType ownerIdx(OwnerId, OwnerIdx);
        BLOG_D("THive::TTxCreateTablet::Execute " << RequestData.ShortDebugString());
        SideEffects.Reset(Self->SelfId());
        ErrorReason = NKikimrHive::ERROR_REASON_UNKNOWN;
        for (const auto& domain : AllowedDomains) {
            if (!Self->SeenDomain(domain)) {
                ++Self->ConfigurationGeneration;
            }
        }
        if (ObjectDomain) {
            if (!Self->SeenDomain(ObjectDomain)) {
                ++Self->ConfigurationGeneration;
            }
        }
        if (Self->BlockedOwners.count(OwnerId) != 0) {
            BLOG_W("THive::TTxCreateTablet::Execute Owner " << OwnerId << " is blocked");
            ReplyToSender(NKikimrProto::BLOCKED);
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        // check if tablet has already been created

        {
            auto itOwner = Self->OwnerToTablet.find(ownerIdx);
            if (itOwner != Self->OwnerToTablet.end()) { // tablet is already created
                const ui64 tabletId = itOwner->second;
                TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
                if (tablet != nullptr) {
                    // make sure tablet type matches the requested one
                    TTabletTypes::EType existingTabletType = tablet->Type;
                    TabletId = tabletId;
                    if (existingTabletType != TabletType || tablet->SeizedByChild) {
                        if (tablet->SeizedByChild) {
                            BLOG_W("THive::TTxCreateTablet::Execute Existing tablet " << tablet->ToString() << " seized by child - operation postponed");
                            PostponeCreateTablet();
                        } else {
                            BLOG_ERROR("THive::TTxCreateTablet::Execute Existing tablet " << tablet->ToString() << " has different type");
                            ReplyToSender(NKikimrProto::ERROR);
                        }
                        return true;
                    }
                    BLOG_D("THive::TTxCreateTablet::Execute TabletId: " << TabletId << " State: " << ETabletStateName(tablet->State));

                    if (!ValidateChannelsBinding(*tablet)) {
                        BLOG_ERROR("THive::TTxCreateTablet::Execute Existing tablet " << tablet->ToString() << " has invalid channel bindings");
                        ReplyToSender(NKikimrProto::ERROR);
                        return true;
                    }

                    if (BootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL) {
                        // Make sure any running tablets are stopped
                        for (TFollowerTabletInfo& follower : tablet->Followers) {
                            follower.InitiateStop(SideEffects);
                        }
                        tablet->InitiateStop(SideEffects);
                    }

                    if (tablet->State == ETabletState::StoppingInGroupAssignment) {
                        tablet->State = ETabletState::GroupAssignment;
                    } else if (tablet->State == ETabletState::Stopping || tablet->State == ETabletState::Stopped) {
                        tablet->State = ETabletState::ReadyToWork;
                    }

                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::State>(tablet->State);
                    tablet->ActorsToNotify.push_back(Sender);
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::ActorsToNotify>(tablet->ActorsToNotify);
                    tablet->NodeFilter.AllowedNodes = AllowedNodeIds;
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::AllowedNodes>(tablet->NodeFilter.AllowedNodes);
                    tablet->AssignDomains(ObjectDomain, AllowedDomains);
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::ObjectDomain>(ObjectDomain);
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::AllowedDomains>(AllowedDomains);
                    tablet->ObjectId = {OwnerId, ObjectId};
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::ObjectID>(ObjectId);
                    tablet->BootMode = BootMode;
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::BootMode>(BootMode);
                    tablet->BalancerPolicy = BalancerPolicy;
                    db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::BalancerPolicy>(BalancerPolicy);

                    UpdateChannelsBinding(*tablet, db);

                    auto itFollowerGroup = tablet->FollowerGroups.begin();
                    for (const auto& srcFollowerGroup : FollowerGroups) {
                        TFollowerGroup* followerGroup;
                        if (itFollowerGroup != tablet->FollowerGroups.end()) {
                            followerGroup = &*itFollowerGroup;
                            ++itFollowerGroup;
                        } else {
                            followerGroup = &tablet->AddFollowerGroup();
                            itFollowerGroup = tablet->FollowerGroups.end();
                        }
                        *followerGroup = srcFollowerGroup;

                        TVector<ui32> allowedDataCenters;
                        for (const TDataCenterId& dc : followerGroup->NodeFilter.AllowedDataCenters) {
                            allowedDataCenters.push_back(DataCenterFromString(dc));
                        }
                        db.Table<Schema::TabletFollowerGroup>().Key(TabletId, followerGroup->Id).Update(
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerGroup->GetRawFollowerCount()),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(followerGroup->AllowLeaderPromotion),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(followerGroup->AllowClientRead),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedNodes>(followerGroup->NodeFilter.AllowedNodes),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenters>(allowedDataCenters),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenterIds>(followerGroup->NodeFilter.AllowedDataCenters),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireAllDataCenters>(followerGroup->RequireAllDataCenters),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>(followerGroup->FollowerCountPerDataCenter),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireDifferentNodes>(followerGroup->RequireDifferentNodes));
                    }

                    auto followerGroupsEnd = itFollowerGroup;
                    for (; itFollowerGroup != tablet->FollowerGroups.end(); ++itFollowerGroup) {
                        db.Table<Schema::TabletFollowerGroup>().Key(TabletId, itFollowerGroup->Id).Delete();
                    }
                    tablet->FollowerGroups.erase(followerGroupsEnd, tablet->FollowerGroups.end());

                    Self->CreateTabletFollowers(*tablet, db, SideEffects);
                    ProcessTablet(*tablet);

                    BLOG_D("THive::TTxCreateTablet::Execute Existing tablet " << tablet->ToString() << " has been successfully updated");
                    ReplyToSender(NKikimrProto::OK);
                    return true;
                }
            } else if (RequestData.HasTabletID()) {
                TTabletId tabletId = RequestData.GetTabletID();
                if (Self->CheckForForwardTabletRequest(tabletId, ForwardRequest)) {
                    TabletId = tabletId;
                    ReplyToSender(NKikimrProto::INVALID_OWNER); // actually this status from blob storage, but I think it fits this situation perfectly
                    return true; // abort transaction
                }
            }
        }

        switch((TTabletTypes::EType)TabletType) {
        case TTabletTypes::BSController:
            BLOG_ERROR("THive::TTxCreateTablet::Execute Cannot create such tablet");
            ReplyToSender(NKikimrProto::ERROR);
            return true;
        default:
            break;
        };

        std::vector<TSequencer::TOwnerType> modified;
        auto tabletIdIndex = Self->Sequencer.AllocateElement(modified);
        if (tabletIdIndex == TSequencer::NO_ELEMENT) {
            BLOG_W("THive::TTxCreateTablet::Execute CreateTablet Postponed");
            PostponeCreateTablet();
            RequestFreeSequence();
            return true;
        } else {
            TabletId = MakeTabletID(true, tabletIdIndex);
            BLOG_D("Hive " << Self->TabletID() << " allocated TabletId " << TabletId << " from TabletIdIndex " << tabletIdIndex);
            Y_ABORT_UNLESS(Self->Tablets.count(TabletId) == 0);
            for (auto owner : modified) {
                auto sequence = Self->Sequencer.GetSequence(owner);
                db.Table<Schema::Sequences>().Key(owner).Update(
                            NIceDb::TUpdate<Schema::Sequences::Begin>(sequence.Begin),
                            NIceDb::TUpdate<Schema::Sequences::Next>(sequence.Next),
                            NIceDb::TUpdate<Schema::Sequences::End>(sequence.End));
            }
            ///// remove after upgrade to sub-hives
            Self->NextTabletId = tabletIdIndex + 1;
            db.Table<Schema::State>().Key(TSchemeIds::State::NextTabletId).Update<Schema::State::Value>(Self->NextTabletId);
            ///// remove after upgrade to sub-hives
        }

        TInstant now = TlsActivationContext->Now();

        // insert entry for new tablet
        TLeaderTabletInfo& tablet = Self->GetTablet(TabletId);
        tablet.NodeId = 0;
        tablet.SetType((TTabletTypes::EType)TabletType);
        tablet.KnownGeneration = 0; // because we will increase it on start
        tablet.State = ETabletState::GroupAssignment;
        tablet.ActorsToNotify.push_back(Sender);
        tablet.NodeFilter.AllowedNodes = AllowedNodeIds;
        tablet.NodeFilter.AllowedDataCenters = AllowedDataCenterIds;
        tablet.Owner = ownerIdx;
        tablet.DataCentersPreference = DataCentersPreference;
        tablet.BootMode = BootMode;
        tablet.ObjectId = {OwnerId, ObjectId};
        tablet.AssignDomains(ObjectDomain, AllowedDomains);
        tablet.Statistics.SetLastAliveTimestamp(now.MilliSeconds());
        tablet.BalancerPolicy = BalancerPolicy;

        TVector<ui32> allowedDataCenters;
        for (const TDataCenterId& dc : tablet.NodeFilter.AllowedDataCenters) {
            allowedDataCenters.push_back(DataCenterFromString(dc));
        }
        db.Table<Schema::Tablet>().Key(TabletId).Update(NIceDb::TUpdate<Schema::Tablet::Owner>(tablet.Owner),
                                                        NIceDb::TUpdate<Schema::Tablet::LeaderNode>(tablet.NodeId),
                                                        NIceDb::TUpdate<Schema::Tablet::TabletType>(tablet.Type),
                                                        NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(tablet.KnownGeneration),
                                                        NIceDb::TUpdate<Schema::Tablet::State>(tablet.State),
                                                        NIceDb::TUpdate<Schema::Tablet::ActorsToNotify>(TVector<TActorId>(1, Sender)),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedNodes>(tablet.NodeFilter.AllowedNodes),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedDataCenters>(allowedDataCenters),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedDataCenterIds>(tablet.NodeFilter.AllowedDataCenters),
                                                        NIceDb::TUpdate<Schema::Tablet::DataCentersPreference>(tablet.DataCentersPreference),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedDomains>(AllowedDomains),
                                                        NIceDb::TUpdate<Schema::Tablet::BootMode>(tablet.BootMode),
                                                        NIceDb::TUpdate<Schema::Tablet::ObjectID>(tablet.ObjectId.second),
                                                        NIceDb::TUpdate<Schema::Tablet::ObjectDomain>(ObjectDomain),
                                                        NIceDb::TUpdate<Schema::Tablet::Statistics>(tablet.Statistics),
                                                        NIceDb::TUpdate<Schema::Tablet::BalancerPolicy>(tablet.BalancerPolicy));

        Self->PendingCreateTablets.erase({OwnerId, OwnerIdx});

        if (TabletCategory.HasTabletCategoryID()) {
            TTabletCategoryInfo* tabletCategory = nullptr;

            db.Table<Schema::TabletCategory>().Key(TabletCategory.GetTabletCategoryID()).Update();
            tabletCategory = &Self->GetTabletCategory(TabletCategory.GetTabletCategoryID());
            if (TabletCategory.HasMaxDisconnectTimeout()) {
                db.Table<Schema::TabletCategory>().Key(TabletCategory.GetTabletCategoryID()).Update<Schema::TabletCategory::MaxDisconnectTimeout>(TabletCategory.GetMaxDisconnectTimeout());
                tabletCategory->MaxDisconnectTimeout = TabletCategory.GetMaxDisconnectTimeout();
            }
            if (TabletCategory.HasStickTogetherInDC()) {
                db.Table<Schema::TabletCategory>().Key(TabletCategory.GetTabletCategoryID()).Update<Schema::TabletCategory::StickTogetherInDC>(TabletCategory.GetStickTogetherInDC());
                tabletCategory->StickTogetherInDC = TabletCategory.GetStickTogetherInDC();
            }
            db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::Category>(TabletCategory.GetTabletCategoryID());
            tablet.Category = tabletCategory;
        } else if (Self->CurrentConfig.GetSystemTabletCategoryId() != 0 && Self->IsSystemTablet(tablet.Type)){
            tablet.Category = &Self->GetTabletCategory(Self->CurrentConfig.GetSystemTabletCategoryId());
        }

        if (tablet.Category != nullptr) {
            tablet.Category->Tablets.insert(&tablet);
        }

        NKikimrTabletBase::TMetrics resourceValues;

        resourceValues.CopyFrom(Self->GetDefaultResourceValuesForTabletType(tablet.Type));
        BLOG_D("THive::TTxCreateTablet::Execute; Default resources after merge for type " << tablet.Type << ": {" << resourceValues.ShortDebugString() << "}");
        if (IsValidObjectId(tablet.ObjectId)) {
            resourceValues.MergeFrom(Self->GetDefaultResourceValuesForObject(tablet.ObjectId));
            BLOG_D("THive::TTxCreateTablet::Execute; Default resources after merge for object " << tablet.ObjectId << ": {" << resourceValues.ShortDebugString() << "}");
        }
        // TODO: provide Hive with resource profile used by the tablet instead of default one.
        resourceValues.MergeFrom(Self->GetDefaultResourceValuesForProfile(tablet.Type, "default"));
        BLOG_D("THive::TTxCreateTablet::Execute; Default resources after merge for profile 'default': {" << resourceValues.ShortDebugString() << "}");
        if (resourceValues.ByteSize() == 0) {
            resourceValues.SetStorage(1ULL << 30); // 1 GB
            resourceValues.SetReadThroughput(10ULL << 20); // 10 MB/s
            resourceValues.SetWriteThroughput(10ULL << 20); // 10 MB/s
        }
        tablet.UpdateResourceUsage(resourceValues);
        tablet.BoundChannels.clear();
        tablet.TabletStorageInfo.Reset(new TTabletStorageInfo(tablet.Id, tablet.Type));
        tablet.TabletStorageInfo->TenantPathId = tablet.GetTenant();

        UpdateChannelsBinding(tablet, db);

        for (const auto& srcFollowerGroup : FollowerGroups) {
            TFollowerGroup& followerGroup = tablet.AddFollowerGroup();
            followerGroup = srcFollowerGroup;

            TVector<ui32> allowedDataCenters;
            for (const TDataCenterId& dc : followerGroup.NodeFilter.AllowedDataCenters) {
                allowedDataCenters.push_back(DataCenterFromString(dc));
            }
            db.Table<Schema::TabletFollowerGroup>().Key(TabletId, followerGroup.Id).Update(
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerGroup.GetRawFollowerCount()),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(followerGroup.AllowLeaderPromotion),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(followerGroup.AllowClientRead),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedNodes>(followerGroup.NodeFilter.AllowedNodes),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenters>(allowedDataCenters),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedDataCenterIds>(followerGroup.NodeFilter.AllowedDataCenters),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireAllDataCenters>(followerGroup.RequireAllDataCenters),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>(followerGroup.FollowerCountPerDataCenter));
        }

        Self->CreateTabletFollowers(tablet, db, SideEffects);
        Self->OwnerToTablet.emplace(ownerIdx, TabletId);
        Self->ObjectToTabletMetrics[tablet.ObjectId].IncreaseCount();
        Self->TabletTypeToTabletMetrics[tablet.Type].IncreaseCount();

        ReplyToSender(NKikimrProto::OK);

        if (tablet.Type == TTabletTypes::Hive) {
            auto itSubDomain = Self->Domains.find(tablet.ObjectDomain);
            if (itSubDomain != Self->Domains.end()) {
                if (itSubDomain->second.HiveId == 0) {
                    itSubDomain->second.HiveId = tablet.Id;
                }
                Self->Execute(Self->CreateUpdateDomain(tablet.ObjectDomain));
            }
        }

        ProcessTablet(tablet);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const TOwnerIdxType::TValueType ownerIdx(OwnerId, OwnerIdx);
        BLOG_D("THive::TTxCreateTablet::Complete " << ownerIdx << " TabletId: " << TabletId << " SideEffects: " << SideEffects);
        SideEffects.Complete(ctx);
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_FREE].Set(Self->Sequencer.FreeSize());
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_ALLOCATED].Set(Self->Sequencer.AllocatedSequencesSize());
    }
};

ITransaction* THive::CreateCreateTablet(NKikimrHive::TEvCreateTablet rec, const TActorId& sender, const ui64 cookie) {
    return new TTxCreateTablet(std::move(rec), sender, cookie, this);
}

} // NHive
} // NKikimr
