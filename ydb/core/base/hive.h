#pragma once
#include "defs.h"
#include "blobstorage.h"
#include "events.h"
#include "storage_pools.h"
#include "subdomain.h"
#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/base/tablet.h>
#include <util/stream/str.h>

namespace NKikimr {
    struct TEvHive {
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

            TString ToString() const {
                TStringStream str;
                str << "{EvBootTablet TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
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

            TString ToString() const {
                TStringStream str;
                str << "{EvBootTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                str << " Msg: " << Record.GetStatusMsg();
                str << "}";
                return str.Str();
            }
        };

        struct TEvCreateTablet : public TEventPB<TEvCreateTablet, NKikimrHive::TEvCreateTablet, EvCreateTablet> {
            TEvCreateTablet()
            {}

            TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
                            TTabletTypes::EType tabletType,
                            const TChannelsBindings& bindedChannels) {
                Record.SetOwner(ownerId);
                Record.SetOwnerIdx(ownerIdx);
                Record.SetTabletType(tabletType);
                for (auto& channel : bindedChannels) {
                    *Record.AddBindedChannels() = channel;
                }
            }

            // deprecated
            TEvCreateTablet(ui64 ownerId,
                            ui64 ownerIdx,
                            TTabletTypes::EType tabletType) {
                Record.SetOwner(ownerId);
                Record.SetOwnerIdx(ownerIdx);
                Record.SetTabletType(tabletType);
            }

            // deprecated
            TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
                            TTabletTypes::EType tabletType,
                            const TChannelsBindings& bindedChannels,
                            const TVector<ui32>& allowedNodeIDs) {
                Record.SetOwner(ownerId);
                Record.SetOwnerIdx(ownerIdx);
                Record.SetTabletType(tabletType);
                for (ui32 nodeId : allowedNodeIDs) {
                    Record.AddAllowedNodeIDs(nodeId);
                }
                for (auto& channel : bindedChannels) {
                    *Record.AddBindedChannels() = channel;
                }
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvCreateTablet Owner: " << Record.GetOwner();
                str << " OwnerIdx: " << Record.GetOwnerIdx();
                str << " TabletType: " << Record.GetTabletType();
                str << " ChannelsProfile: " << Record.GetChannelsProfile();

                ui32 bindedChannelsSize = Record.BindedChannelsSize();
                if (bindedChannelsSize) {
                    str << " BindedChannels: {" << Record.GetBindedChannels(0);
                    for (size_t i = 1; i < bindedChannelsSize; ++i) {
                        str << ", " << Record.GetBindedChannels(i);
                    }
                    str << "}";
                }

                ui32 allowedNodeIDsSize = Record.AllowedNodeIDsSize();
                if (allowedNodeIDsSize) {
                    str << " AllowenNodeIDs: {" << Record.GetAllowedNodeIDs(0);
                    for (size_t i = 1; i < allowedNodeIDsSize; ++i) {
                        str << ", " << Record.GetAllowedNodeIDs(i);
                    }
                    str << "}";
                }
                str << "}";
                return str.Str();
            }
        };

        struct TEvCreateTabletReply : TEventPB<TEvCreateTabletReply, NKikimrHive::TEvCreateTabletReply, EvCreateTabletReply> {
            TEvCreateTabletReply() = default;

            TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx) {
                Record.SetStatus(status);
                Record.SetOwner(ownerId);
                Record.SetOwnerIdx(ownerIdx);
            }

            TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx, ui64 tabletId)
                : TEvCreateTabletReply(status, ownerId, ownerIdx)
            {
                if (tabletId != 0)
                    Record.SetTabletID(tabletId);
            }

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
                    NKikimrHive::EErrorReason errorReason)
                : TEvCreateTabletReply(status, ownerId, ownerIdx, tabletId, origin)
            {
                if (errorReason != NKikimrHive::ERROR_REASON_UNKNOWN) {
                    Record.SetErrorReason(errorReason);
                }
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvCreateTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                str << " Owner: " << Record.GetOwner();
                str << " OwnerIdx: " << Record.GetOwnerIdx();
                if (Record.HasTabletID()) {
                    str << " TabletID: " << Record.GetTabletID();
                }
                if (Record.HasOrigin()) {
                    str << " Origin: " << Record.GetOrigin();
                }
                str << "}";
                return str.Str();
            }
        };

        struct TEvTabletCreationResult : public TEventPB<
                TEvTabletCreationResult, NKikimrHive::TEvTabletCreationResult, EvTabletCreationResult> {
            TEvTabletCreationResult()
            {}

            TEvTabletCreationResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
                Record.SetStatus(status);
                if (tabletId != 0) {
                    Record.SetTabletID(tabletId);
                }
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvTabletCreationResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                if (Record.HasTabletID()) {
                    str << " TabletID: " << Record.GetTabletID();
                }
                str << "}";
                return str.Str();
            }
        };

        struct TEvStopTablet : public TEventPB<TEvStopTablet, NKikimrHive::TEvStopTablet, EvStopTablet> {
            TEvStopTablet()
            {}

            TEvStopTablet(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TEvStopTablet(ui64 tabletId, const TActorId &actorToNotify) {
                Record.SetTabletID(tabletId);
                ActorIdToProto(actorToNotify, Record.MutableActorToNotify());
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvStopTablet TabletID: " << Record.GetTabletID();
                if (Record.HasActorToNotify()) {
                    str << " ActorToNotify: " << ActorIdFromProto(Record.GetActorToNotify()).ToString();
                }
                str << "}";
                return str.Str();
            }
        };

        struct TEvStopTabletResult : TEventPB<TEvStopTabletResult, NKikimrHive::TEvStopTabletResult, EvStopTabletResult> {
            TEvStopTabletResult() = default;

            TEvStopTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
                Record.SetStatus(status);
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvStopTabletResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus());
                str << " TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvResumeTabletResult : TEventPB<TEvResumeTabletResult, NKikimrHive::TEvResumeTabletResult, EvResumeTabletResult> {
            TEvResumeTabletResult() = default;

            TEvResumeTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
                Record.SetStatus(status);
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvResumeTabletResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus());
                str << " TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvAdoptTablet
                : public TEventPB<TEvAdoptTablet, NKikimrHive::TEvAdoptTablet, EvAdoptTable> {
            TEvAdoptTablet()
            {}

            TEvAdoptTablet(ui64 tabletId, ui64 prevOwner, ui64 prevOwnerIdx, TTabletTypes::EType tabletType, ui64 owner, ui64 ownerIdx) {
                Record.SetTabletID(tabletId);
                Record.SetPrevOwner(prevOwner);
                Record.SetPrevOwnerIdx(prevOwnerIdx);
                Record.SetTabletType(tabletType);
                Record.SetOwner(owner);
                Record.SetOwnerIdx(ownerIdx);
            }

            TString ToString() const {
                TStringStream str;
                str << "{TEvAdoptTablet TabletID: " << Record.GetTabletID();
                str << " PrevOwner: " << Record.GetPrevOwner();
                str << " PrevOwnerIdx: " << Record.GetPrevOwnerIdx();
                str << " Owner: " << Record.GetOwner();
                str << " OwnerIdx: " << Record.GetOwnerIdx();
                str << "}";
                return str.Str();
            }
        };

        struct TEvAdoptTabletReply : TEventPB<TEvAdoptTabletReply, NKikimrHive::TEvAdoptTabletReply, EvAdoptTabletReply> {
            TEvAdoptTabletReply() = default;

            TEvAdoptTabletReply(NKikimrProto::EReplyStatus status, ui64 tablet, ui64 owner, ui64 ownerIdx, const TString explain, ui64 origin) {
                Record.SetStatus(status);
                Record.SetTabletID(tablet);
                Record.SetOwner(owner);
                Record.SetOwnerIdx(ownerIdx);
                Record.SetExplain(explain);
                Record.SetOrigin(origin);
            }

            TString ToString() const {
                TStringStream str;
                str << "{TEvAdoptTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                str << " TabletID: " << Record.GetTabletID();
                str << " Explain: " << Record.GetExplain();
                str << "}";
                return str.Str();
            }
        };

        struct TEvReconfigureTablet
                : public TEventPB<TEvReconfigureTablet, NKikimrHive::TEvReconfigureTablet, EvReconfigureTablet> {
            TEvReconfigureTablet()
            {}

            TEvReconfigureTablet(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvReconfigureTablet TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvReconfigureTabletReply : public TEventPB<TEvReconfigureTabletReply,
                NKikimrHive::TEvReconfigureTabletReply, EvReconfigureTabletReply> {
            TEvReconfigureTabletReply()
            {}

            TEvReconfigureTabletReply(NKikimrProto::EReplyStatus status, ui64 tabletId) {
                Record.SetStatus(status);
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvReconfigureTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                str << " TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvDeleteTablet : public TEventPB<TEvDeleteTablet, NKikimrHive::TEvDeleteTablet, EvDeleteTablet> {
            TEvDeleteTablet()
            {}

            TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 txId) {
                Record.SetShardOwnerId(ownerId);
                Record.AddShardLocalIdx(ownerIdx);
                Record.SetTxId_Deprecated(txId);
            }

            TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 tabletId, ui64 txId) {
                Record.SetShardOwnerId(ownerId);
                Record.AddShardLocalIdx(ownerIdx);
                Record.AddTabletID(tabletId);
                Record.SetTxId_Deprecated(txId);
            }
        };

        struct TEvDeleteTabletReply : public TEventPB<TEvDeleteTabletReply,
                NKikimrHive::TEvDeleteTabletReply, EvDeleteTabletReply> {
            TEvDeleteTabletReply()
            {}

            TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 txId, ui64 ownerId, const TVector<ui64>& localIdxs) {
                Record.SetStatus(status);
                Record.SetOrigin(hiveID);
                Record.SetTxId_Deprecated(txId);
                Record.SetShardOwnerId(ownerId);
                for (auto& idx: localIdxs) {
                    Record.AddShardLocalIdx(idx);
                }
            }

            TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, const NKikimrHive::TEvDeleteTablet& request) {
                Record.SetStatus(status);
                Record.SetOrigin(hiveID);
                Record.SetTxId_Deprecated(request.GetTxId_Deprecated());
                Record.SetShardOwnerId(request.GetShardOwnerId());
                for (auto idx : request.GetShardLocalIdx()) {
                    Record.AddShardLocalIdx(idx);
                }
            }
        };

        struct TEvDeleteOwnerTablets : public TEventPB<TEvDeleteOwnerTablets,
                                                       NKikimrHive::TEvDeleteOwnerTablets, EvDeleteOwnerTablets> {
            TEvDeleteOwnerTablets()
            {}

            TEvDeleteOwnerTablets(ui64 ownerId, ui64 txId) {
                Record.SetOwner(ownerId);
                Record.SetTxId(txId);
            }
        };

        struct TEvDeleteOwnerTabletsReply : public TEventPB<TEvDeleteOwnerTabletsReply,
                                                      NKikimrHive::TEvDeleteOwnerTabletsReply, EvDeleteOwnerTabletsReply> {
            TEvDeleteOwnerTabletsReply()
            {}

            TEvDeleteOwnerTabletsReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 ownerId, ui64 txId) {
                Record.SetStatus(status);
                Record.SetOwner(ownerId);
                Record.SetTxId(txId);
                Record.SetOrigin(hiveID);
            }
        };

        struct TEvLookupChannelInfo : TEventPB<TEvLookupChannelInfo, NKikimrHive::TEvLookupChannelInfo, EvLookupChannelInfo> {
            TEvLookupChannelInfo() = default;

            TEvLookupChannelInfo(ui64 tabletId) {
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvLookupChannelInfo TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvChannelInfo : TEventPB<TEvChannelInfo, NKikimrHive::TEvChannelInfo, EvChannelInfo> {
            TEvChannelInfo() = default;

            TEvChannelInfo(NKikimrProto::EReplyStatus status, ui64 tabletId) {
                Record.SetStatus(status);
                Record.SetTabletID(tabletId);
            }

            TString ToString() const {
                TStringStream str;
                str << "{EvChannelInfo Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
                str << " TabletID: " << Record.GetTabletID();
                str << "}";
                return str.Str();
            }
        };

        struct TEvTabletMetrics : public TEventPB<TEvTabletMetrics, NKikimrHive::TEvTabletMetrics, EvTabletMetrics> {

        };

        struct TEvReassignTablet : TEventPB<TEvReassignTablet, NKikimrHive::TEvReassignTablet, EvReassignTablet> {
            TEvReassignTablet() = default;

            TEvReassignTablet(ui64 tabletId,
                              const TVector<ui32>& channels = {},
                              const TVector<ui32>& forcedGroupIds = {})
            {
                Record.SetTabletID(tabletId);
                for (ui32 channel : channels) {
                    Record.AddChannels(channel);
                }
                for (ui32 forcedGroupId : forcedGroupIds) {
                    Record.AddForcedGroupIDs(forcedGroupId);
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

            TEvRequestHiveInfo(TRequestHiveInfoInitializer initializer) {
                Record.SetTabletID(initializer.TabletId);
                Record.SetReturnFollowers(initializer.ReturnFollowers);
                Record.SetReturnMetrics(initializer.ReturnMetrics);
                Record.SetReturnChannelHistory(initializer.ReturnChannelHistory);
            }

            TEvRequestHiveInfo() = default;

            TEvRequestHiveInfo(bool returnFollowers) {
                Record.SetReturnFollowers(returnFollowers);
            }
            TEvRequestHiveInfo(ui64 tabletId, bool returnFollowers) {
                Record.SetTabletID(tabletId);
                Record.SetReturnFollowers(returnFollowers);
            }
        };

        struct TEvResponseHiveInfo : TEventPB<TEvResponseHiveInfo, NKikimrHive::TEvResponseHiveInfo, EvResponseHiveInfo> {};

        struct TEvLookupTablet : public TEventPB<TEvLookupTablet, NKikimrHive::TEvLookupTablet, EvLookupTablet> {
            TEvLookupTablet() = default;

            TEvLookupTablet(ui64 ownerId, ui64 ownerIdx) {
                Record.SetOwner(ownerId);
                Record.SetOwnerIdx(ownerIdx);
            }
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

            TEvGetTabletStorageInfoResult(ui64 tabletId, NKikimrProto::EReplyStatus status) {
                Record.SetTabletID(tabletId);
                Record.SetStatus(status);
            }

            TEvGetTabletStorageInfoResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage)
                : TEvGetTabletStorageInfoResult(tabletId, status)
            {
                Record.SetStatusMessage(statusMessage);
            }

            TEvGetTabletStorageInfoResult(ui64 tabletId, const TTabletStorageInfo& info) {
                Record.SetTabletID(tabletId);
                Record.SetStatus(NKikimrProto::OK);
                TabletStorageInfoToProto(info, Record.MutableInfo());
            }
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

            TEvLockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage) {
                Record.SetTabletID(tabletId);
                Record.SetStatus(status);
                Record.SetStatusMessage(statusMessage);
            }
        };

        struct TEvLockTabletExecutionLost : public TEventPB<TEvLockTabletExecutionLost,
                NKikimrHive::TEvLockTabletExecutionLost, EvLockTabletExecutionLost>
        {
            TEvLockTabletExecutionLost() = default;

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

            TEvUnlockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage) {
                Record.SetTabletID(tabletId);
                Record.SetStatus(status);
                Record.SetStatusMessage(statusMessage);
            }
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

            TEvRequestTabletIdSequence(ui64 ownerId, ui64 ownerIdx, size_t size) {
                Record.MutableOwner()->SetOwner(ownerId);
                Record.MutableOwner()->SetOwnerIdx(ownerIdx);
                Record.SetSize(size);
            }
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

            TEvQueryMigrationReply(NKikimrHive::EMigrationState state, i32 progress) {
                Record.SetMigrationState(state);
                Record.SetMigrationProgress(progress);
            }
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
    };

    IActor* CreateDefaultHive(const TActorId &tablet, TTabletStorageInfo *info);
}
