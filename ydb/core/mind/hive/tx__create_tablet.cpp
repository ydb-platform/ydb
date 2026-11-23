#include "hive_impl.h"
#include "hive_log.h"

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxCreateTablet : public TTransactionBase<THive> {
    NKikimrHive::TEvCreateTablet RequestData;
    const ui64 OwnerId;
    ui64 OwnerIdx;
    TVector<ui64> BatchIndices;
    THashSet<ui64> ConflictingBindings;
    THolder<TEvHive::TEvCreateTabletReply> BatchReply;
    TInstant ReceivedAt;
    const TTabletTypes::EType TabletType;

    const TActorId Sender;
    const ui64 Cookie;

    NKikimrHive::EErrorReason ErrorReason;
    ui64 TabletId;
    TObjectId ObjectId;
    TSubDomainKey ObjectDomain;

    TChannelsBindings BoundChannels;
    TVector<TNodeId> AllowedNodeIds;
    NKikimrHive::TDataCentersGroup AllowedDataCenterIds;
    NKikimrHive::TDataCentersPreference DataCentersPreference;
    TVector<TSubDomainKey> AllowedDomains;
    NKikimrHive::TTabletCategory TabletCategory;
    TVector<NKikimrHive::TFollowerGroup> FollowerGroups;
    NKikimrHive::ETabletBootMode BootMode;
    NKikimrHive::TForwardRequest ForwardRequest;

    NKikimrHive::EBalancerPolicy BalancerPolicy;
    bool IsBackup = false;

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
        , IsBackup(RequestData.GetIsBackup())
    {
        const ui32 allowedNodeIdsSize = RequestData.AllowedNodeIDsSize();
        AllowedNodeIds.reserve(allowedNodeIdsSize);
        for (ui32 idx = 0; idx < allowedNodeIdsSize; ++idx) {
            AllowedNodeIds.push_back(RequestData.GetAllowedNodeIDs(idx));
        }
        Sort(AllowedNodeIds);

        AllowedDataCenterIds.MutableDataCenter()->CopyFrom(RequestData.GetAllowedDataCenters());
        Sort(*AllowedDataCenterIds.MutableDataCenter());

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
        if (RequestData.HasCount()) {
            BatchIndices.reserve(RequestData.GetCount());
            for (ui32 offset = 0; offset < RequestData.GetCount(); ++offset) {
                BatchIndices.push_back(OwnerIdx + offset);
            }
        } else {
            BatchIndices.assign(RequestData.GetOwnerIdxs().begin(), RequestData.GetOwnerIdxs().end());
        }
        if (!BatchIndices.empty()) {
            ReceivedAt = TlsActivationContext->Now();
        }
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
            if (tablet.IsReadyToReassignTablet()) {
                tablet.State = ETabletState::GroupAssignment;
            }
            tablet.ChannelProfileReassignReason = NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO;
            tablet.BoundChannels = BoundChannels;
            for (auto& bind : tablet.BoundChannels) {
                Self->InitDefaultChannelBind(bind);
            }
            db.Table<Schema::Tablet>().Key(TabletId)
                .Update<Schema::Tablet::State, Schema::Tablet::ReassignReason, Schema::Tablet::ActorsToNotify>(
                    tablet.State, NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO,
                    BatchReply ? TVector<TActorId>() : TVector<TActorId>{Sender}
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
        if (BatchReply) {
            // The caller retries only the unfinished indices.
            ReplyToSender(NKikimrProto::TRYLATER);
            return;
        }
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
        YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute sending create tablet reply",
            {"logPrefix", GetLogPrefix()},
            {"tabletId", TabletId},
            {"status", NKikimrProto::EReplyStatus_Name(status)});
        if (BatchReply) {
            auto* result = BatchReply->Record.AddResults();
            result->SetOwnerIdx(OwnerIdx);
            result->SetStatus(status);
            if (status == NKikimrProto::OK) {
                result->SetTabletID(TabletId);
            }
            if (ErrorReason != NKikimrHive::ERROR_REASON_UNKNOWN) {
                result->SetErrorReason(ErrorReason);
            }
            if (ForwardRequest.HasHiveTabletId()) {
                *result->MutableForwardRequest() = ForwardRequest;
            }
            return;
        }
        Y_ABORT_UNLESS(!!Sender);
        THolder<TEvHive::TEvCreateTabletReply> reply = MakeHolder<TEvHive::TEvCreateTabletReply>(status, OwnerId, OwnerIdx, TabletId, Self->TabletID(), ErrorReason);
        if (ForwardRequest.HasHiveTabletId()) {
            reply->Record.MutableForwardRequest()->CopyFrom(ForwardRequest);
        }
        SideEffects.Send(Sender, reply.Release(), 0, Cookie);
    }

    void ProcessTablet(TLeaderTabletInfo& tablet) {
        if (tablet.IsReadyToAssignGroups()) {
            if (!std::exchange(tablet.IsMarkedForReassign, true)) {
                Self->UpdateCounterTabletsReassigning(+1);
            }
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

    TTxType GetTxType() const override {
        return BatchIndices.empty() ? NHive::TXTYPE_CREATE_TABLET : NHive::TXTYPE_CREATE_TABLET_BATCH;
    }

    // BoundChannels in memory includes the current Hive defaults. The database
    // retains the original binding, including which resources the owner specified.
    bool ReadReplayBindings(NIceDb::TNiceDb& db) {
        ConflictingBindings.clear();
        for (const ui64 index : BatchIndices) {
            const auto existing = Self->OwnerToTablet.find({OwnerId, index});
            if (existing == Self->OwnerToTablet.end()) {
                continue;
            }
            using TChannels = Schema::TabletChannel;
            auto rows = db.Table<TChannels>().Range(existing->second).Select<TChannels::Binding, TChannels::StoragePool>();
            if (!rows.IsReady()) {
                return false;
            }
            size_t channel = 0;
            while (!rows.EndOfSet() && channel < BoundChannels.size()) {
                auto binding = rows.GetValueOrDefault<TChannels::Binding>();
                if (!rows.HaveValue<TChannels::Binding>()) {
                    binding.SetStoragePoolName(rows.GetValue<TChannels::StoragePool>());
                }
                if (binding.SerializeAsString() != BoundChannels[channel].SerializeAsString()) {
                    break;
                }
                ++channel;
                if (!rows.Next()) {
                    return false;
                }
            }
            if (!rows.EndOfSet() || channel != BoundChannels.size()) {
                ConflictingBindings.insert(existing->second);
            }
        }
        return true;
    }

    bool MatchesCategoryOptions(const TTabletCategoryInfo& category) const {
        return (!TabletCategory.HasMaxDisconnectTimeout() || category.MaxDisconnectTimeout == TabletCategory.GetMaxDisconnectTimeout())
            && (!TabletCategory.HasStickTogetherInDC() || category.StickTogetherInDC == TabletCategory.GetStickTogetherInDC());
    }

    bool MatchesCreateParameters(const TLeaderTabletInfo& tablet) const {
        const auto domain = ObjectDomain ? ObjectDomain
            : AllowedDomains.empty() ? Self->GetRootDomainKey() : AllowedDomains.front();
        const auto allowed = AllowedDomains.empty() ? TVector<TSubDomainKey>{domain} : AllowedDomains;
        if (tablet.Type != TabletType || tablet.ObjectId != TFullObjectId{OwnerId, ObjectId}
                || tablet.ObjectDomain != domain || tablet.NodeFilter.AllowedDomains != allowed
                || tablet.IsBackup != IsBackup || tablet.BootMode != BootMode || tablet.BalancerPolicy != BalancerPolicy
                || tablet.NodeFilter.AllowedNodes != AllowedNodeIds
                || tablet.NodeFilter.AllowedDataCenters.SerializeAsString() != AllowedDataCenterIds.SerializeAsString()
                || tablet.DataCentersPreference.SerializeAsString() != DataCentersPreference.SerializeAsString()
                || ConflictingBindings.contains(tablet.Id) || tablet.FollowerGroups.size() != FollowerGroups.size()) {
            return false;
        }

        auto category = TabletCategory.HasTabletCategoryID() ? TabletCategory.GetTabletCategoryID()
            : Self->IsSystemTablet(tablet.Type) ? Self->CurrentConfig.GetSystemTabletCategoryId() : 0;
        if (category != (tablet.Category ? tablet.Category->Id : 0)
                || (tablet.Category && !MatchesCategoryOptions(*tablet.Category))
                || (!tablet.Category && (TabletCategory.HasMaxDisconnectTimeout() || TabletCategory.HasStickTogetherInDC()))) {
            return false;
        }
        auto existing = tablet.FollowerGroups.begin();
        for (const auto& group : FollowerGroups) {
            const auto& current = *existing++;
            if (current.GetRawFollowerCount() != group.GetFollowerCount()
                    || current.AllowLeaderPromotion != group.GetAllowLeaderPromotion()
                    || current.AllowClientRead != group.GetAllowClientRead()
                    || current.RequireAllDataCenters != group.GetRequireAllDataCenters()
                    || current.RequireDifferentNodes != group.GetRequireDifferentNodes()
                    || current.FollowerCountPerDataCenter != group.GetFollowerCountPerDataCenter()
                    || current.LocalNodeOnly != group.GetLocalNodeOnly()
                    || !std::ranges::equal(current.NodeFilter.AllowedNodes, group.GetAllowedNodeIDs())
                    || !std::ranges::equal(current.NodeFilter.AllowedDataCenters.GetDataCenter(), group.GetAllowedDataCenters())) {
                return false;
            }
        }
        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute processing create tablet request",
            {"logPrefix", GetLogPrefix()},
            {"request", RequestData.ShortDebugString()});
        SideEffects.Reset(Self->SelfId());
        NIceDb::TNiceDb db(txc.DB);
        if (!BatchIndices.empty() && !ReadReplayBindings(db)) {
            return false;
        }
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
        if (!BatchIndices.empty()) {
            BatchReply = MakeHolder<TEvHive::TEvCreateTabletReply>();
            BatchReply->Record.SetOwner(OwnerId);
            BatchReply->Record.SetOrigin(Self->TabletID());
            BatchReply->Record.SetStatus(NKikimrProto::OK);
            for (const ui64 index : BatchIndices) {
                CreateTablet(index, db);
            }
            SideEffects.Send(Sender, BatchReply.Release(), 0, Cookie);
        } else {
            CreateTablet(RequestData.GetOwnerIdx(), db);
        }
        return true;
    }

    void CreateTablet(ui64 index, NIceDb::TNiceDb& db) {
        OwnerIdx = index;
        TabletId = 0;
        ErrorReason = NKikimrHive::ERROR_REASON_UNKNOWN;
        ForwardRequest.Clear();
        const TOwnerIdxType::TValueType ownerIdx(OwnerId, OwnerIdx);
        if (Self->BlockedOwners.count(OwnerId) != 0) {
            YDB_LOG_WARN("THive::TTxCreateTablet::Execute owner is blocked, rejecting create",
                {"logPrefix", GetLogPrefix()},
                {"ownerId", OwnerId});
            ReplyToSender(NKikimrProto::BLOCKED);
            return;
        }
        // check if tablet has already been created

        {
            auto itOwner = Self->OwnerToTablet.find(ownerIdx);
            if (itOwner != Self->OwnerToTablet.end()) { // tablet is already created
                const ui64 tabletId = itOwner->second;
                if (BatchReply) {
                    auto* existing = Self->FindTablet(tabletId);
                    TabletId = tabletId;
                    if (existing && (existing->SeizedByChild || existing->NeedToReleaseFromParent)) {
                        ReplyToSender(NKikimrProto::TRYLATER);
                    } else if (!existing || !MatchesCreateParameters(*existing)) {
                        ErrorReason = NKikimrHive::ERROR_REASON_CREATE_PARAMETERS_MISMATCH;
                        ReplyToSender(NKikimrProto::ERROR);
                    } else {
                        ReplyToSender(NKikimrProto::OK);
                    }
                    return;
                }
                TLeaderTabletInfo* tablet = Self->FindTablet(tabletId);
                if (tablet != nullptr) {
                    // make sure tablet type matches the requested one
                    TTabletTypes::EType existingTabletType = tablet->Type;
                    TabletId = tabletId;
                    if (existingTabletType != TabletType || tablet->SeizedByChild) {
                        if (tablet->SeizedByChild) {
                            YDB_LOG_WARN("THive::TTxCreateTablet::Execute existing tablet seized by child, operation postponed",
                                {"logPrefix", GetLogPrefix()},
                                {"tabletInfo", tablet->ToString()});
                            PostponeCreateTablet();
                        } else {
                            YDB_LOG_ERROR("THive::TTxCreateTablet::Execute existing tablet has different type",
                                {"logPrefix", GetLogPrefix()},
                                {"tabletInfo", tablet->ToString()});
                            ReplyToSender(NKikimrProto::ERROR);
                        }
                        return;
                    }
                    YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute updating existing tablet",
                        {"logPrefix", GetLogPrefix()},
                        {"tabletId", TabletId},
                        {"state", ETabletStateName(tablet->State)});

                    if (!ValidateChannelsBinding(*tablet)) {
                        YDB_LOG_ERROR("THive::TTxCreateTablet::Execute existing tablet has invalid channel bindings",
                            {"logPrefix", GetLogPrefix()},
                            {"tabletInfo", tablet->ToString()});
                        ReplyToSender(NKikimrProto::ERROR);
                        return;
                    }

                    if (BootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL) {
                        // Make sure any running tablets are stopped
                        for (TFollowerTabletInfo& follower : tablet->Followers) {
                            follower.InitiateStop(SideEffects, EHiveEventReason::BootingSuppressed);
                        }
                        tablet->InitiateStop(SideEffects, EHiveEventReason::BootingSuppressed);
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
                    if (RequestData.HasIsBackup()) {
                        tablet->IsBackup = IsBackup;
                        db.Table<Schema::Tablet>().Key(TabletId).Update<Schema::Tablet::IsBackup>(IsBackup);
                    }

                    UpdateChannelsBinding(*tablet, db);

                    for (auto& [_, dc] : Self->DataCenters) {
                        for (const auto& followerGroup : tablet->FollowerGroups) {
                            dc.Followers.erase({TabletId, followerGroup.Id});
                        }
                    }

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

                        db.Table<Schema::TabletFollowerGroup>().Key(TabletId, followerGroup->Id).Update(
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerGroup->GetRawFollowerCount()),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(followerGroup->AllowLeaderPromotion),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(followerGroup->AllowClientRead),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedNodes>(followerGroup->NodeFilter.AllowedNodes),
                                    NIceDb::TUpdate<Schema::TabletFollowerGroup::NewAllowedDataCenterIds>(followerGroup->NodeFilter.AllowedDataCenters),
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

                    YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute existing tablet updated successfully",
                        {"logPrefix", GetLogPrefix()},
                        {"tabletInfo", tablet->ToString()});
                    ReplyToSender(NKikimrProto::OK);
                    return;
                }
            } else if (RequestData.HasTabletID()) {
                TTabletId tabletId = RequestData.GetTabletID();
                if (Self->CheckForForwardTabletRequest(tabletId, ForwardRequest)) {
                    TabletId = tabletId;
                    ReplyToSender(NKikimrProto::INVALID_OWNER); // actually this status from blob storage, but I think it fits this situation perfectly
                    return; // abort transaction
                }
            }
        }

        if (BatchReply && Self->AreWeRootHive()) {
            // Match SchemeShard's tenant, shared, root placement order. A retry
            // after migration may reach the old Hive without a known TabletID.
            const auto* domain = Self->FindDomain(ObjectDomain);
            if ((!domain || !domain->HiveId) && !AllowedDomains.empty()) {
                domain = Self->FindDomain(AllowedDomains.front());
            }
            if (domain && domain->HiveId && domain->HiveId != Self->TabletID()) {
                ForwardRequest.SetHiveTabletId(domain->HiveId);
                ReplyToSender(NKikimrProto::INVALID_OWNER);
                return;
            }
        }
        if (BatchReply && TabletCategory.HasTabletCategoryID()) {
            const auto category = Self->TabletCategories.find(TabletCategory.GetTabletCategoryID());
            if (category != Self->TabletCategories.end() && !MatchesCategoryOptions(category->second)) {
                // Changing a shared category here would also change other tablets.
                ErrorReason = NKikimrHive::ERROR_REASON_CREATE_PARAMETERS_MISMATCH;
                ReplyToSender(NKikimrProto::ERROR);
                return;
            }
        }
        switch((TTabletTypes::EType)TabletType) {
        case TTabletTypes::BSController:
            YDB_LOG_ERROR("THive::TTxCreateTablet::Execute cannot create tablet of this type",
                {"logPrefix", GetLogPrefix()});
            ReplyToSender(NKikimrProto::ERROR);
            return;
        default:
            break;
        };

        std::vector<TSequencer::TOwnerType> modified;
        auto tabletIdIndex = Self->Sequencer.AllocateElement(modified);
        if (tabletIdIndex == TSequencer::NO_ELEMENT) {
            YDB_LOG_WARN("THive::TTxCreateTablet::Execute create tablet postponed, no free tablet id",
                {"logPrefix", GetLogPrefix()});
            PostponeCreateTablet();
            RequestFreeSequence();
            return;
        } else {
            TabletId = MakeTabletID(true, tabletIdIndex);
            YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute allocated tablet id from index",
                {"logPrefix", GetLogPrefix()},
                {"selfTabletId", Self->TabletID()},
                {"tabletId", TabletId},
                {"tabletIdIndex", tabletIdIndex});
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
        // The batch reply acknowledges metadata. Only the legacy protocol also
        // subscribes the caller to one creation notification per tablet.
        if (!BatchReply) {
            tablet.ActorsToNotify.push_back(Sender);
        }
        tablet.NodeFilter.AllowedNodes = AllowedNodeIds;
        tablet.NodeFilter.AllowedDataCenters = AllowedDataCenterIds;
        tablet.Owner = ownerIdx;
        tablet.DataCentersPreference = DataCentersPreference;
        tablet.BootMode = BootMode;
        tablet.ObjectId = {OwnerId, ObjectId};
        tablet.AssignDomains(ObjectDomain, AllowedDomains);
        tablet.Statistics.SetLastAliveTimestamp(now.MilliSeconds());
        tablet.BalancerPolicy = BalancerPolicy;
        tablet.IsBackup = IsBackup;
        Self->RecordTabletEvent(tablet, EHiveEventType::Created, EHiveEventReason::OwnerRequest,
            TStringBuilder() << "type=" << TTabletTypes::TypeToStr(tablet.Type) << " channels=" << BoundChannels.size());

        TDomainInfo* domain = Self->FindDomain(ObjectDomain);
        if (domain && domain->Stopped) {
            tablet.State = ETabletState::Stopped;
            tablet.BecomeStopped(EHiveEventReason::TenantStopped);
            tablet.StoppedByTenant = true;
        }

        db.Table<Schema::Tablet>().Key(TabletId).Update(NIceDb::TUpdate<Schema::Tablet::Owner>(tablet.Owner),
                                                        NIceDb::TUpdate<Schema::Tablet::LeaderNode>(tablet.NodeId),
                                                        NIceDb::TUpdate<Schema::Tablet::TabletType>(tablet.Type),
                                                        NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(tablet.KnownGeneration),
                                                        NIceDb::TUpdate<Schema::Tablet::State>(tablet.State),
                                                        NIceDb::TUpdate<Schema::Tablet::ActorsToNotify>(tablet.ActorsToNotify),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedNodes>(tablet.NodeFilter.AllowedNodes),
                                                        NIceDb::TUpdate<Schema::Tablet::NewAllowedDataCenterIds>(tablet.NodeFilter.AllowedDataCenters),
                                                        NIceDb::TUpdate<Schema::Tablet::DataCentersPreference>(tablet.DataCentersPreference),
                                                        NIceDb::TUpdate<Schema::Tablet::AllowedDomains>(AllowedDomains),
                                                        NIceDb::TUpdate<Schema::Tablet::BootMode>(tablet.BootMode),
                                                        NIceDb::TUpdate<Schema::Tablet::ObjectID>(tablet.ObjectId.second),
                                                        NIceDb::TUpdate<Schema::Tablet::ObjectDomain>(ObjectDomain),
                                                        NIceDb::TUpdate<Schema::Tablet::Statistics>(tablet.Statistics),
                                                        NIceDb::TUpdate<Schema::Tablet::BalancerPolicy>(tablet.BalancerPolicy),
                                                        NIceDb::TUpdate<Schema::Tablet::StoppedByTenant>(tablet.StoppedByTenant),
                                                        NIceDb::TUpdate<Schema::Tablet::IsBackup>(tablet.IsBackup),
                                                        NIceDb::TUpdate<Schema::Tablet::ConfirmedStorageVersion>(tablet.ConfirmedStorageVersion));

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

        Self->GetDefaultResourceValuesForTabletType(tablet.Type).ToProto(&resourceValues);
        YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute default resources after merge for tablet type",
            {"logPrefix", GetLogPrefix()},
            {"tabletType", tablet.Type},
            {"resourceValues", resourceValues.ShortDebugString()});
        if (IsValidObjectId(tablet.ObjectId)) {
            Self->GetDefaultResourceValuesForObject(tablet.ObjectId).ToProto(&resourceValues);
            YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute default resources after merge for object",
                {"logPrefix", GetLogPrefix()},
                {"objectId", tablet.ObjectId},
                {"resourceValues", resourceValues.ShortDebugString()});
        }
        // TODO: provide Hive with resource profile used by the tablet instead of default one.
        Self->GetDefaultResourceValuesForProfile(tablet.Type, "default").ToProto(&resourceValues);
        YDB_LOG_DEBUG("THive::TTxCreateTablet::Execute default resources after merge for default profile",
            {"logPrefix", GetLogPrefix()},
            {"resourceValues", resourceValues.ShortDebugString()});
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

            db.Table<Schema::TabletFollowerGroup>().Key(TabletId, followerGroup.Id).Update(
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCount>(followerGroup.GetRawFollowerCount()),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowLeaderPromotion>(followerGroup.AllowLeaderPromotion),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowClientRead>(followerGroup.AllowClientRead),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::AllowedNodes>(followerGroup.NodeFilter.AllowedNodes),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::NewAllowedDataCenterIds>(followerGroup.NodeFilter.AllowedDataCenters),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireAllDataCenters>(followerGroup.RequireAllDataCenters),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::FollowerCountPerDataCenter>(followerGroup.FollowerCountPerDataCenter),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::LocalNodeOnly>(followerGroup.LocalNodeOnly),
                        NIceDb::TUpdate<Schema::TabletFollowerGroup::RequireDifferentNodes>(followerGroup.RequireDifferentNodes));
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
    }

    void Complete(const TActorContext& ctx) override {
        const TOwnerIdxType::TValueType ownerIdx(OwnerId, OwnerIdx);
        YDB_LOG_DEBUG("THive::TTxCreateTablet::Complete",
            {"logPrefix", GetLogPrefix()},
            {"ownerIdx", ownerIdx},
            {"tabletId", TabletId},
            {"sideEffects", SideEffects});
        if (!BatchIndices.empty()) {
            auto& counters = Self->TabletCounters->Percentile();
            counters[NHive::COUNTER_CREATE_TABLET_BATCH_SIZE].IncrementFor(BatchIndices.size());
            counters[NHive::COUNTER_CREATE_TABLET_BATCH_DURATION_MS].IncrementFor((ctx.Now() - ReceivedAt).MilliSeconds());
        }
        SideEffects.Complete(ctx, Self->Requests);
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_FREE].Set(Self->Sequencer.FreeSize());
        Self->TabletCounters->Simple()[NHive::COUNTER_SEQUENCE_ALLOCATED].Set(Self->Sequencer.AllocatedSequencesSize());
    }
};

ITransaction* THive::CreateCreateTablet(NKikimrHive::TEvCreateTablet rec, const TActorId& sender, const ui64 cookie) {
    return new TTxCreateTablet(std::move(rec), sender, cookie, this);
}

} // NHive
} // NKikimr
