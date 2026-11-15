#pragma once
#include "blobstorage_tablet_types.h"
#include "defs.h"
#include "events.h"
#include "storage_pools.h"
#include "subdomain.h"
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/base/tablet.h>

namespace NKikimr {
    namespace TEvHive {
        enum EEv {
            // requests
            EvBootTablet = EventSpaceBegin(TKikimrEvents::ES_HIVE),
            EvCreateTablet,
            EvForgetTablet,
            EvReconfigureTablet,
            EvLookupChannelInfo,
            EvStopTablet,

            EvTabletMetrics = EvStopTablet + 7, // review 194375
            EvReassignTablet,
            EvInitiateBlockStorage,
            EvDeleteTablet,
            EvRequestHiveInfo,
            EvLookupTablet,
            EvDrainNode,
            EvFillNode,
            EvInitiateDeleteStorage,
            EvGetTabletStorageInfo,
            EvLockTabletExecution,
            EvUnlockTabletExecution,
            EvInitiateTabletExternalBoot,
            EvRequestHiveDomainStats,
            EvAdoptTable,
            EvInvalidateStoragePools,
            EvRequestHiveNodeStats,
            EvRequestHiveStorageStats,
            EvDeleteOwnerTablets,
            EvRequestTabletIdSequence,
            EvSeizeTablets,
            EvReleaseTablets,
            EvConfigureHive,
            EvInitMigration,
            EvQueryMigration,
            EvRequestTabletOwners,
            EvReassignOnDecommitGroup,
            EvUpdateTabletsObject,
            EvUpdateDomain,
            EvRequestTabletDistribution,
            EvRequestScaleRecommendation,
            EvConfigureScaleRecommender,
            EvRequestDrainInfo,
            EvSetDown,
            EvShrinkStoragePool,

            // replies
            EvBootTabletReply = EvBootTablet + 512,
            EvCreateTabletReply,
            EvForgetTabletReply,
            EvReconfigureTabletReply,
            EvChannelInfo,
            EvStopTabletResult,
            EvDeleteTabletReply,
            EvTabletCreationResult,
            EvResponseHiveInfo,
            EvDrainNodeResult,
            EvFillNodeResult,
            EvGetTabletStorageInfoResult,
            EvGetTabletStorageInfoRegistered,
            EvLockTabletExecutionResult,
            EvUnlockTabletExecutionResult,
            EvLockTabletExecutionLost,
            EvResponseHiveDomainStats,
            EvAdoptTabletReply,
            EvResponseHiveNodeStats,
            EvResponseHiveStorageStats,
            EvDeleteOwnerTabletsReply,
            EvResponseTabletIdSequence,
            EvResumeTabletResult,
            EvSeizeTabletsReply,
            EvReleaseTabletsReply,
            EvInitMigrationReply,
            EvQueryMigrationReply,
            EvTabletOwnersReply,
            EvInvalidateStoragePoolsReply,
            EvReassignOnDecommitGroupReply,
            EvUpdateTabletsObjectReply,
            EvUpdateDomainReply,
            EvResponseTabletDistribution,
            EvResponseScaleRecommendation,
            EvConfigureScaleRecommenderReply,
            EvDrainNodeAck,
            EvResponseDrainInfo,
            EvSetDownReply,
            EvShrinkStoragePoolReply,
            EvShrinkStoragePoolDone,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_HIVE),
            "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_HIVE)");

        struct TEvBootTablet : public TEventPB<TEvBootTablet, NKikimrHive::TEvBootTablet, EvBootTablet> {
            TEvBootTablet()
            {}

            TEvBootTablet(ui64 tabletId)
            {
                Record.SetTabletID(tabletId);
            }

            TString ToString() const;
        };

        struct TEvBootTabletReply
                : public TEventPB<TEvBootTabletReply, NKikimrHive::TEvBootTabletReply, EvBootTabletReply> {
            TEvBootTabletReply()
            {}

            TEvBootTabletReply(NKikimrProto::EReplyStatus status, const TString& msg = {})
            {
                Record.SetStatus(status);
                Record.SetStatusMsg(msg);
            }

            TString ToString() const;
        };

        struct TEvCreateTablet : public TEventPB<TEvCreateTablet, NKikimrHive::TEvCreateTablet, EvCreateTablet> {
            TEvCreateTablet()
            {}

            TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
                            TTabletTypes::EType tabletType,
                            const TChannelsBindings& bindedChannels);

            // deprecated
            TEvCreateTablet(ui64 ownerId,
                            ui64 ownerIdx,
                            TTabletTypes::EType tabletType);

            // deprecated
            TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
                            TTabletTypes::EType tabletType,
                            const TChannelsBindings& bindedChannels,
                            const TVector<ui32>& allowedNodeIDs);

            TString ToString() const;
        };

        struct TEvCreateTabletReply : TEventPB<TEvCreateTabletReply, NKikimrHive::TEvCreateTabletReply, EvCreateTabletReply> {
            TEvCreateTabletReply() = default;

            TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx);

            TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx, ui64 tabletId);

            TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx, ui64 tabletId, ui64 origin)
                : TEvCreateTabletReply(status, ownerId, ownerIdx, tabletId)
            {
                Record.SetOrigin(origin);
            }

            TEvCreateTabletReply(
                    NKikimrProto::EReplyStatus status,
                    ui64 ownerId,
                    ui64 ownerIdx,
                    ui64 tabletId,
                    ui64 origin,
                    NKikimrHive::EErrorReason errorReason);

            TString ToString() const;
        };

        struct TEvTabletCreationResult : public TEventPB<
                TEvTabletCreationResult, NKikimrHive::TEvTabletCreationResult, EvTabletCreationResult> {
            TEvTabletCreationResult()
            {}

            TEvTabletCreationResult(NKikimrProto::EReplyStatus status, ui64 tabletId);

            TString ToString() const;
        };

        struct TEvStopTablet : public TEventPB<TEvStopTablet, NKikimrHive::TEvStopTablet, EvStopTablet> {
            TEvStopTablet()
            {}

            TEvStopTablet(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TEvStopTablet(ui64 tabletId, const TActorId &actorToNotify);

            TString ToString() const;
        };

        struct TEvStopTabletResult : TEventPB<TEvStopTabletResult, NKikimrHive::TEvStopTabletResult, EvStopTabletResult> {
            TEvStopTabletResult() = default;

            TEvStopTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId);

            TString ToString() const;
        };

        struct TEvResumeTabletResult : TEventPB<TEvResumeTabletResult, NKikimrHive::TEvResumeTabletResult, EvResumeTabletResult> {
            TEvResumeTabletResult() = default;

            TEvResumeTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId);

            TString ToString() const;
        };

        struct TEvAdoptTablet
                : public TEventPB<TEvAdoptTablet, NKikimrHive::TEvAdoptTablet, EvAdoptTable> {
            TEvAdoptTablet()
            {}

            TEvAdoptTablet(ui64 tabletId, ui64 prevOwner, ui64 prevOwnerIdx, TTabletTypes::EType tabletType, ui64 owner, ui64 ownerIdx);

            TString ToString() const;
        };

        struct TEvAdoptTabletReply : TEventPB<TEvAdoptTabletReply, NKikimrHive::TEvAdoptTabletReply, EvAdoptTabletReply> {
            TEvAdoptTabletReply() = default;

            TEvAdoptTabletReply(NKikimrProto::EReplyStatus status, ui64 tablet, ui64 owner, ui64 ownerIdx, const TString explain, ui64 origin);

            TString ToString() const;
        };

        struct TEvReconfigureTablet
                : public TEventPB<TEvReconfigureTablet, NKikimrHive::TEvReconfigureTablet, EvReconfigureTablet> {
            TEvReconfigureTablet()
            {}

            TEvReconfigureTablet(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TString ToString() const;
        };

        struct TEvReconfigureTabletReply : public TEventPB<TEvReconfigureTabletReply,
                NKikimrHive::TEvReconfigureTabletReply, EvReconfigureTabletReply> {
            TEvReconfigureTabletReply()
            {}

            TEvReconfigureTabletReply(NKikimrProto::EReplyStatus status, ui64 tabletId);

            TString ToString() const;
        };

        struct TEvDeleteTablet : public TEventPB<TEvDeleteTablet, NKikimrHive::TEvDeleteTablet, EvDeleteTablet> {
            TEvDeleteTablet()
            {}

            TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 txId);

            TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 tabletId, ui64 txId);
        };

        struct TEvDeleteTabletReply : public TEventPB<TEvDeleteTabletReply,
                NKikimrHive::TEvDeleteTabletReply, EvDeleteTabletReply> {
            TEvDeleteTabletReply()
            {}

            TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 txId, ui64 ownerId, const TVector<ui64>& localIdxs);

            TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, const NKikimrHive::TEvDeleteTablet& request);
        };

        struct TEvDeleteOwnerTablets : public TEventPB<TEvDeleteOwnerTablets,
                                                       NKikimrHive::TEvDeleteOwnerTablets, EvDeleteOwnerTablets> {
            TEvDeleteOwnerTablets()
            {}

            TEvDeleteOwnerTablets(ui64 ownerId, ui64 txId);
        };

        struct TEvDeleteOwnerTabletsReply : public TEventPB<TEvDeleteOwnerTabletsReply,
                                                      NKikimrHive::TEvDeleteOwnerTabletsReply, EvDeleteOwnerTabletsReply> {
            TEvDeleteOwnerTabletsReply()
            {}

            TEvDeleteOwnerTabletsReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 ownerId, ui64 txId);
        };

        struct TEvLookupChannelInfo : TEventPB<TEvLookupChannelInfo, NKikimrHive::TEvLookupChannelInfo, EvLookupChannelInfo> {
            TEvLookupChannelInfo() = default;

            TEvLookupChannelInfo(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TString ToString() const;
        };

        struct TEvChannelInfo : TEventPB<TEvChannelInfo, NKikimrHive::TEvChannelInfo, EvChannelInfo> {
            TEvChannelInfo() = default;

            TEvChannelInfo(NKikimrProto::EReplyStatus status, ui64 tabletId);

            TString ToString() const;
        };

        struct TEvTabletMetrics : public TEventPB<TEvTabletMetrics, NKikimrHive::TEvTabletMetrics, EvTabletMetrics> {

        };

        struct TEvReassignTablet : TEventPB<TEvReassignTablet, NKikimrHive::TEvReassignTablet, EvReassignTablet> {
            TEvReassignTablet() = default;

            TEvReassignTablet(ui64 tabletId,
                              const TVector<ui32>& channels = {},
                              const TVector<ui32>& forcedGroupIds = {},
                              bool async = false)
            {
                Record.SetTabletID(tabletId);
                for (ui32 channel : channels) {
                    Record.AddChannels(channel);
                }
                for (ui32 forcedGroupId : forcedGroupIds) {
                    Record.AddForcedGroupIDs(forcedGroupId);
                }
                if (async) {
                    Record.SetAsync(async);
                }
            }
        };

        struct TEvReassignTabletSpace : TEvReassignTablet {
            TEvReassignTabletSpace(ui64 tabletId,
                                   const TVector<ui32>& channels = {},
                                   const TVector<ui32>& forcedGroupIds = {})
                : TEvReassignTablet(tabletId, channels, forcedGroupIds)
            {
                Record.SetReassignReason(NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_SPACE);
            }
        };

        struct TEvInitiateBlockStorage : public TEventLocal<TEvInitiateBlockStorage, EvInitiateBlockStorage> {
            ui64 TabletId;

            TEvInitiateBlockStorage(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvInitiateDeleteStorage : public TEventLocal<TEvInitiateDeleteStorage, EvInitiateDeleteStorage> {
            ui64 TabletId;

            TEvInitiateDeleteStorage(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvRequestHiveInfo : TEventPB<TEvRequestHiveInfo, NKikimrHive::TEvRequestHiveInfo, EvRequestHiveInfo> {
            struct TRequestHiveInfoInitializer {
                ui64 TabletId = 0;
                bool ReturnFollowers = false;
                bool ReturnMetrics = false;
                bool ReturnChannelHistory = false;
            };

            TEvRequestHiveInfo(TRequestHiveInfoInitializer initializer);

            TEvRequestHiveInfo() = default;

            TEvRequestHiveInfo(bool returnFollowers) {
                Record.SetReturnFollowers(returnFollowers);
            }
            TEvRequestHiveInfo(ui64 tabletId, bool returnFollowers);
        };

        struct TEvResponseHiveInfo : TEventPB<TEvResponseHiveInfo, NKikimrHive::TEvResponseHiveInfo, EvResponseHiveInfo> {};

        struct TEvLookupTablet : public TEventPB<TEvLookupTablet, NKikimrHive::TEvLookupTablet, EvLookupTablet> {
            TEvLookupTablet() = default;

            TEvLookupTablet(ui64 ownerId, ui64 ownerIdx);
        };

        using TEvCutTabletHistory = TEvTablet::TEvCutTabletHistory;

        struct TEvDrainNode : TEventPB<TEvDrainNode, NKikimrHive::TEvDrainNode, EvDrainNode> {
            TEvDrainNode() = default;

            TEvDrainNode(ui32 nodeId) {
                Record.SetNodeID(nodeId);
            }
        };

        struct TEvDrainNodeResult : TEventPB<TEvDrainNodeResult, NKikimrHive::TEvDrainNodeResult, EvDrainNodeResult> {
            TEvDrainNodeResult() = default;

            TEvDrainNodeResult(NKikimrProto::EReplyStatus status) {
                Record.SetStatus(status);
            }

            TEvDrainNodeResult(NKikimrProto::EReplyStatus status, ui32 movements)
                : TEvDrainNodeResult(status)
            {
                Record.SetMovements(movements);
            }
        };

        struct TEvDrainNodeAck : TEventPB<TEvDrainNodeAck, NKikimrHive::TEvDrainNodeAck, EvDrainNodeAck> {
            TEvDrainNodeAck() = default;

            TEvDrainNodeAck(ui64 seqNo) {
                Record.SetSeqNo(seqNo);
            }
        };

        struct TEvFillNode : TEventPB<TEvFillNode, NKikimrHive::TEvFillNode, EvFillNode> {
            TEvFillNode() = default;

            TEvFillNode(ui32 nodeId) {
                Record.SetNodeID(nodeId);
            }
        };

        struct TEvFillNodeResult : TEventPB<TEvFillNodeResult, NKikimrHive::TEvFillNodeResult, EvFillNodeResult> {
            TEvFillNodeResult() = default;

            TEvFillNodeResult(NKikimrProto::EReplyStatus status) {
                Record.SetStatus(status);
            }
        };

        struct TEvGetTabletStorageInfo : public TEventPB<TEvGetTabletStorageInfo,
                NKikimrHive::TEvGetTabletStorageInfo, EvGetTabletStorageInfo>
        {
            TEvGetTabletStorageInfo() = default;

            explicit TEvGetTabletStorageInfo(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvGetTabletStorageInfoResult : public TEventPB<TEvGetTabletStorageInfoResult,
                NKikimrHive::TEvGetTabletStorageInfoResult, EvGetTabletStorageInfoResult>
        {
            TEvGetTabletStorageInfoResult() = default;

            TEvGetTabletStorageInfoResult(ui64 tabletId, NKikimrProto::EReplyStatus status);

            TEvGetTabletStorageInfoResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage)
                : TEvGetTabletStorageInfoResult(tabletId, status)
            {
                Record.SetStatusMessage(statusMessage);
            }

            TEvGetTabletStorageInfoResult(ui64 tabletId, const TTabletStorageInfo& info);
        };

        struct TEvGetTabletStorageInfoRegistered : public TEventPB<TEvGetTabletStorageInfoRegistered,
                NKikimrHive::TEvGetTabletStorageInfoRegistered, EvGetTabletStorageInfoRegistered>
        {
            TEvGetTabletStorageInfoRegistered() = default;

            explicit TEvGetTabletStorageInfoRegistered(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvLockTabletExecution : public TEventPB<TEvLockTabletExecution,
                NKikimrHive::TEvLockTabletExecution, EvLockTabletExecution>
        {
            TEvLockTabletExecution() = default;

            explicit TEvLockTabletExecution(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvLockTabletExecutionResult : public TEventPB<TEvLockTabletExecutionResult,
                NKikimrHive::TEvLockTabletExecutionResult, EvLockTabletExecutionResult>
        {
            TEvLockTabletExecutionResult() = default;

            TEvLockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage);

            TString ToString() const override;
        };

        struct TEvLockTabletExecutionLost : public TEventPB<TEvLockTabletExecutionLost,
                NKikimrHive::TEvLockTabletExecutionLost, EvLockTabletExecutionLost>
        {
            TEvLockTabletExecutionLost() = default;

            TEvLockTabletExecutionLost(ui64 tabletId, NKikimrHive::ELockLostReason reason);

            explicit TEvLockTabletExecutionLost(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvUnlockTabletExecution : public TEventPB<TEvUnlockTabletExecution,
                NKikimrHive::TEvUnlockTabletExecution, EvUnlockTabletExecution>
        {
            TEvUnlockTabletExecution() = default;

            explicit TEvUnlockTabletExecution(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvUnlockTabletExecutionResult : public TEventPB<TEvUnlockTabletExecutionResult,
                NKikimrHive::TEvUnlockTabletExecutionResult, EvUnlockTabletExecutionResult>
        {
            TEvUnlockTabletExecutionResult() = default;

            TEvUnlockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage);
        };

        struct TEvInitiateTabletExternalBoot : public TEventPB<TEvInitiateTabletExternalBoot,
                NKikimrHive::TEvBootTablet, EvInitiateTabletExternalBoot>
        {
            TEvInitiateTabletExternalBoot() = default;

            explicit TEvInitiateTabletExternalBoot(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }
        };

        struct TEvRequestHiveDomainStats : TEventPB<TEvRequestHiveDomainStats, NKikimrHive::TEvRequestHiveDomainStats, EvRequestHiveDomainStats> {
            TEvRequestHiveDomainStats() = default;
        };

        struct TEvResponseHiveDomainStats : TEventPB<TEvResponseHiveDomainStats, NKikimrHive::TEvResponseHiveDomainStats, EvResponseHiveDomainStats> {
            TEvResponseHiveDomainStats() = default;
        };

        struct TEvInvalidateStoragePools : TEventPB<TEvInvalidateStoragePools, NKikimrHive::TEvInvalidateStoragePools, EvInvalidateStoragePools> {
            TEvInvalidateStoragePools() = default;
        };

        struct TEvInvalidateStoragePoolsReply : TEventPB<TEvInvalidateStoragePoolsReply, NKikimrHive::TEvInvalidateStoragePoolsReply, EvInvalidateStoragePoolsReply> {
            TEvInvalidateStoragePoolsReply() = default;
        };

        struct TEvRequestHiveNodeStats : TEventPB<TEvRequestHiveNodeStats, NKikimrHive::TEvRequestHiveNodeStats, EvRequestHiveNodeStats> {
            TEvRequestHiveNodeStats() = default;
        };

        struct TEvResponseHiveNodeStats : TEventPB<TEvResponseHiveNodeStats, NKikimrHive::TEvResponseHiveNodeStats, EvResponseHiveNodeStats> {
            TEvResponseHiveNodeStats() = default;
        };

        struct TEvRequestHiveStorageStats : TEventPB<TEvRequestHiveStorageStats, NKikimrHive::TEvRequestHiveStorageStats, EvRequestHiveStorageStats> {
            TEvRequestHiveStorageStats() = default;
        };

        struct TEvResponseHiveStorageStats : TEventPB<TEvResponseHiveStorageStats, NKikimrHive::TEvResponseHiveStorageStats, EvResponseHiveStorageStats> {
            TEvResponseHiveStorageStats() = default;
        };

        struct TEvRequestTabletIdSequence : TEventPB<TEvRequestTabletIdSequence, NKikimrHive::TEvRequestTabletIdSequence, EvRequestTabletIdSequence> {
            TEvRequestTabletIdSequence() = default;

            TEvRequestTabletIdSequence(ui64 ownerId, ui64 ownerIdx, size_t size);
        };

        struct TEvResponseTabletIdSequence : TEventPB<TEvResponseTabletIdSequence, NKikimrHive::TEvResponseTabletIdSequence, EvResponseTabletIdSequence> {
            TEvResponseTabletIdSequence() = default;
        };

        struct TEvSeizeTablets : TEventPB<TEvSeizeTablets, NKikimrHive::TEvSeizeTablets, EvSeizeTablets> {
            TEvSeizeTablets() = default;

            TEvSeizeTablets(const NKikimrHive::TEvSeizeTablets& settings) {
                Record = settings;
            }
        };

        struct TEvSeizeTabletsReply : TEventPB<TEvSeizeTabletsReply, NKikimrHive::TEvSeizeTabletsReply, EvSeizeTabletsReply> {
            TEvSeizeTabletsReply() = default;
        };

        struct TEvReleaseTablets : TEventPB<TEvReleaseTablets, NKikimrHive::TEvReleaseTablets, EvReleaseTablets> {
            TEvReleaseTablets() = default;
        };

        struct TEvReleaseTabletsReply : TEventPB<TEvReleaseTabletsReply, NKikimrHive::TEvReleaseTabletsReply, EvReleaseTabletsReply> {
            TEvReleaseTabletsReply() = default;
        };

        struct TEvConfigureHive : TEventPB<TEvConfigureHive, NKikimrHive::TEvConfigureHive, EvConfigureHive> {
            TEvConfigureHive() = default;

            TEvConfigureHive(TSubDomainKey subdomain) {
                Record.MutableDomain()->CopyFrom(subdomain);
            }
        };

        struct TEvInitMigration : TEventPB<TEvInitMigration, NKikimrHive::TEvInitMigration, EvInitMigration> {
            TEvInitMigration() = default;
        };

        struct TEvInitMigrationReply : TEventPB<TEvInitMigrationReply, NKikimrHive::TEvInitMigrationReply, EvInitMigrationReply> {
            TEvInitMigrationReply() = default;

            TEvInitMigrationReply(NKikimrProto::EReplyStatus status) {
                Record.SetStatus(status);
            }
        };

        struct TEvQueryMigration : TEventPB<TEvQueryMigration, NKikimrHive::TEvQueryMigration, EvQueryMigration> {
            TEvQueryMigration() = default;
        };

        struct TEvQueryMigrationReply : TEventPB<TEvQueryMigrationReply, NKikimrHive::TEvQueryMigrationReply, EvQueryMigrationReply> {
            TEvQueryMigrationReply() = default;

            TEvQueryMigrationReply(NKikimrHive::EMigrationState state, i32 progress);
        };

        struct TEvRequestTabletOwners : TEventPB<TEvRequestTabletOwners, NKikimrHive::TEvRequestTabletOwners, EvRequestTabletOwners> {};
        struct TEvTabletOwnersReply : TEventPB<TEvTabletOwnersReply, NKikimrHive::TEvTabletOwnersReply, EvTabletOwnersReply> {};

        struct TEvReassignOnDecommitGroup : TEventPB<TEvReassignOnDecommitGroup, NKikimrHive::TEvReassignOnDecommitGroup, EvReassignOnDecommitGroup> {
            TEvReassignOnDecommitGroup() = default;

            TEvReassignOnDecommitGroup(ui32 groupId) {
                Record.SetGroupId(groupId);
            }
        };

        struct TEvReassignOnDecommitGroupReply : TEventPB<TEvReassignOnDecommitGroupReply, NKikimrHive::TEvReassignOnDecommitGroupReply, EvReassignOnDecommitGroupReply> {};

        struct TEvUpdateTabletsObject : TEventPB<TEvUpdateTabletsObject, NKikimrHive::TEvUpdateTabletsObject, EvUpdateTabletsObject> {};

        struct TEvUpdateTabletsObjectReply : TEventPB<TEvUpdateTabletsObjectReply, NKikimrHive::TEvUpdateTabletsObjectReply, EvUpdateTabletsObjectReply> {
            TEvUpdateTabletsObjectReply() = default;

            TEvUpdateTabletsObjectReply(NKikimrProto::EReplyStatus status) {
                Record.SetStatus(status);
            }
        };

        struct TEvUpdateDomain : TEventPB<TEvUpdateDomain, NKikimrHive::TEvUpdateDomain, EvUpdateDomain> {};

        struct TEvUpdateDomainReply : TEventPB<TEvUpdateDomainReply, NKikimrHive::TEvUpdateDomainReply, EvUpdateDomainReply> {};

        struct TEvRequestTabletDistribution : TEventPB<TEvRequestTabletDistribution,
            NKikimrHive::TEvRequestTabletDistribution, EvRequestTabletDistribution> {};

        struct TEvResponseTabletDistribution : TEventPB<TEvResponseTabletDistribution,
            NKikimrHive::TEvResponseTabletDistribution, EvResponseTabletDistribution> {};

        struct TEvRequestScaleRecommendation : TEventPB<TEvRequestScaleRecommendation,
            NKikimrHive::TEvRequestScaleRecommendation, EvRequestScaleRecommendation>
        {
            TEvRequestScaleRecommendation() = default;

            TEvRequestScaleRecommendation(TSubDomainKey domainKey) {
                Record.MutableDomainKey()->CopyFrom(domainKey);
            }
        };

        struct TEvResponseScaleRecommendation : TEventPB<TEvResponseScaleRecommendation,
            NKikimrHive::TEvResponseScaleRecommendation, EvResponseScaleRecommendation> {};
        
        struct TEvConfigureScaleRecommender : TEventPB<TEvConfigureScaleRecommender,
            NKikimrHive::TEvConfigureScaleRecommender, EvConfigureScaleRecommender> {};
        
        struct TEvConfigureScaleRecommenderReply : TEventPB<TEvConfigureScaleRecommenderReply,
            NKikimrHive::TEvConfigureScaleRecommenderReply, EvConfigureScaleRecommenderReply> {};

        struct TEvRequestDrainInfo : TEventPB<TEvRequestDrainInfo, NKikimrHive::TEvRequestDrainInfo, EvRequestDrainInfo> {
            TEvRequestDrainInfo() = default;

            TEvRequestDrainInfo(ui32 nodeId) {
                Record.SetNodeId(nodeId);
            }
        };

        struct TEvResponseDrainInfo : TEventPB<TEvResponseDrainInfo, NKikimrHive::TEvResponseDrainInfo, EvResponseDrainInfo> {};

        struct TEvSetDown : TEventPB<TEvSetDown, NKikimrHive::TEvSetDown, EvSetDown> {
            TEvSetDown() = default;

            TEvSetDown(ui32 nodeId, bool down = true);
        };
        struct TEvSetDownReply : TEventPB<TEvSetDownReply, NKikimrHive::TEvSetDownReply, EvSetDownReply> {};

        struct TEvShrinkStoragePool : TEventPB<TEvShrinkStoragePool, NKikimrHive::TEvShrinkStoragePool, EvShrinkStoragePool> {};

        struct TEvShrinkStoragePoolReply : TEventPB<TEvShrinkStoragePoolReply, NKikimrHive::TEvShrinkStoragePoolReply, EvShrinkStoragePoolReply> {};

        struct TEvShrinkStoragePoolDone : TEventPB<TEvShrinkStoragePoolDone, NKikimrHive::TEvShrinkStoragePoolDone, EvShrinkStoragePoolDone> {};
    };

    IActor* CreateDefaultHive(const TActorId &tablet, TTabletStorageInfo *info);
}
