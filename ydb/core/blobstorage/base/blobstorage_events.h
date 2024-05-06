#pragma once
#include "defs.h"

#include "blobstorage_vdiskid.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/blobstorage/pdisk/drivedata_serializer.h>

namespace NKikimrBlobStorage {
    class TStorageConfig;
}

namespace NKikimr {

    struct TEvBlobStorage::TEvControllerUpdateDiskStatus : TEventPB<
            TEvBlobStorage::TEvControllerUpdateDiskStatus,
            NKikimrBlobStorage::TEvControllerUpdateDiskStatus,
            TEvBlobStorage::EvControllerUpdateDiskStatus> {
        TEvControllerUpdateDiskStatus() = default;

        TEvControllerUpdateDiskStatus(const TVDiskID& vDiskId, ui32 nodeId, ui32 pdiskId, ui32 vslotId,
                ui32 satisfactionRankPercent) {
            NKikimrBlobStorage::TVDiskMetrics* metric = Record.AddVDisksMetrics();
            VDiskIDFromVDiskID(vDiskId, metric->MutableVDiskId());
            metric->SetSatisfactionRank(satisfactionRankPercent);
            auto *p = metric->MutableVSlotId();
            p->SetNodeId(nodeId);
            p->SetPDiskId(pdiskId);
            p->SetVSlotId(vslotId);
        }
    };

    struct TEvBlobStorage::TEvControllerRegisterNode : public TEventPB<
        TEvBlobStorage::TEvControllerRegisterNode,
        NKikimrBlobStorage::TEvControllerRegisterNode,
        TEvBlobStorage::EvControllerRegisterNode>
    {
        TEvControllerRegisterNode()
        {}

        TEvControllerRegisterNode(ui32 nodeID, const TVector<ui32>& startedDynamicGroups,
                const TVector<ui32>& groupGenerations, const TVector<NPDisk::TDriveData>& drivesData) {
            Record.SetNodeID(nodeID);
            for (auto groupId: startedDynamicGroups) {
                Record.AddGroups(groupId);
            }
            for (auto generation : groupGenerations) {
                Record.AddGroupGenerations(generation);
            }
            for (const auto& data : drivesData) {
                DriveDataToDriveData(data, Record.AddDrivesData());
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvRegisterNode Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerUpdateNodeDrives : public TEventPB<
        TEvBlobStorage::TEvControllerUpdateNodeDrives,
        NKikimrBlobStorage::TEvControllerUpdateNodeDrives,
        TEvBlobStorage::EvControllerUpdateNodeDrives>
    {
        TEvControllerUpdateNodeDrives()
        {}

        TEvControllerUpdateNodeDrives(ui32 nodeId, const TVector<NPDisk::TDriveData>& drivesData) {
            Record.SetNodeId(nodeId);
            for (const auto& data : drivesData) {
                DriveDataToDriveData(data, Record.AddDrivesData());
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerUpdateNodeDrives Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerNodeServiceSetUpdate : public TEventPB<
        TEvBlobStorage::TEvControllerNodeServiceSetUpdate,
        NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate,
        TEvBlobStorage::EvControllerNodeServiceSetUpdate>
    {
        TEvControllerNodeServiceSetUpdate()
        {}

        TEvControllerNodeServiceSetUpdate(NKikimrProto::EReplyStatus status, ui32 nodeID) {
            Record.SetStatus(status);
            Record.SetNodeID(nodeID);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerNodeServiceSetUpdate";
            str << " Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerGetGroup : public TEventPB<
        TEvBlobStorage::TEvControllerGetGroup,
        NKikimrBlobStorage::TEvControllerGetGroup,
        TEvBlobStorage::EvControllerGetGroup>
    {
        TEvControllerGetGroup()
        {}

        TEvControllerGetGroup(ui32 nodeID, ui32 groupID) {
            Record.SetNodeID(nodeID);
            Record.AddGroupIDs(groupID);
        }

        template<typename TBeginIter, typename TEndIter = TBeginIter>
        TEvControllerGetGroup(ui32 nodeID, TBeginIter beginIter, TEndIter&& endIter) {
            Record.SetNodeID(nodeID);
            while (beginIter != endIter) {
                Record.AddGroupIDs(*beginIter++);
            }
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerGetGroup NodeID# " << Record.GetNodeID();
            str << " GroupIDs.size()# " << Record.GroupIDsSize();
            if (Record.GroupIDsSize()) {
                str << "{";
                for (size_t i = 0; i < Record.GroupIDsSize(); ++i) {
                    str << "GroupID# " << Record.GetGroupIDs((int)i);
                }
                str << "}";
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerSelectGroups : public TEventPB<
        TEvBlobStorage::TEvControllerSelectGroups,
        NKikimrBlobStorage::TEvControllerSelectGroups,
        TEvBlobStorage::EvControllerSelectGroups>
    {
        TEvControllerSelectGroups()
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerSelectGroups}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerSelectGroupsResult : public TEventPB<
        TEvBlobStorage::TEvControllerSelectGroupsResult,
        NKikimrBlobStorage::TEvControllerSelectGroupsResult,
        TEvBlobStorage::EvControllerSelectGroupsResult>
    {
        TEvControllerSelectGroupsResult()
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerSelectGroupsResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvRequestControllerInfo :
            TEventPB<TEvBlobStorage::TEvRequestControllerInfo,
            NKikimrBlobStorage::TEvRequestBSControllerInfo,
            TEvBlobStorage::EvRequestControllerInfo> {
        TEvRequestControllerInfo() = default;

        TEvRequestControllerInfo(ui32 groupId) {
            Record.SetGroupId(groupId);
        }
    };

    struct TEvBlobStorage::TEvResponseControllerInfo :
            TEventPB<TEvBlobStorage::TEvResponseControllerInfo,
            NKikimrBlobStorage::TEvResponseBSControllerInfo,
            TEvBlobStorage::EvResponseControllerInfo> {

        TEvResponseControllerInfo() = default;
    };

    struct TEvBlobStorage::TEvControllerNodeReport: public TEventPB<
        TEvBlobStorage::TEvControllerNodeReport,
        NKikimrBlobStorage::TEvControllerNodeReport,
        TEvBlobStorage::EvControllerNodeReport>
    {
        TEvControllerNodeReport() = default;

        TEvControllerNodeReport(ui32 nodeId) {
            Record.SetNodeId(nodeId);
        }
    };

    struct TEvBlobStorage::TEvControllerConfigRequest : TEventPB<TEvBlobStorage::TEvControllerConfigRequest,
        NKikimrBlobStorage::TEvControllerConfigRequest, TEvBlobStorage::EvControllerConfigRequest>
    {
        bool SelfHeal = false;
        bool GroupLayoutSanitizer = false;

        TEvControllerConfigRequest() = default;
    };

    struct TEvBlobStorage::TEvControllerConfigResponse : TEventPB<TEvBlobStorage::TEvControllerConfigResponse,
        NKikimrBlobStorage::TEvControllerConfigResponse, TEvBlobStorage::EvControllerConfigResponse>
    {
        TEvControllerConfigResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerProposeGroupKey : public TEventPB<
        TEvBlobStorage::TEvControllerProposeGroupKey,
        NKikimrBlobStorage::TEvControllerProposeGroupKey,
        TEvBlobStorage::EvControllerProposeGroupKey>
    {
        TEvControllerProposeGroupKey()
        {}

        TEvControllerProposeGroupKey(ui32 nodeId, ui32 groupId, ui32 lifeCyclePhase,
                const TString& mainKeyId, const TString& encryptedGroupKey, ui64 mainKeyVersion, ui64 groupKeyNonce) {
            Record.SetNodeId(nodeId);
            Record.SetGroupId(groupId);
            Record.SetLifeCyclePhase(lifeCyclePhase);
            Record.SetMainKeyId(mainKeyId);
            Record.SetEncryptedGroupKey(encryptedGroupKey);
            Record.SetMainKeyVersion(mainKeyVersion);
            Record.SetGroupKeyNonce(groupKeyNonce);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerProposeGroupKey Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerUpdateGroupStat : TEventPB<
        TEvBlobStorage::TEvControllerUpdateGroupStat,
        NKikimrBlobStorage::TEvControllerUpdateGroupStat,
        TEvBlobStorage::EvControllerUpdateGroupStat>
    {
    };

    struct TEvBlobStorage::TEvVStatus : public TEventPB<
        TEvBlobStorage::TEvVStatus,
        NKikimrBlobStorage::TEvVStatus,
        TEvBlobStorage::EvVStatus>
    {
        TEvVStatus()
        {}

        TEvVStatus(const TVDiskID &vdisk) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }
    };

    struct TEvBlobStorage::TEvVStatusResult : public TEventPB<
        TEvBlobStorage::TEvVStatusResult,
        NKikimrBlobStorage::TEvVStatusResult,
        TEvBlobStorage::EvVStatusResult>
    {
        TEvVStatusResult() = default;

        TEvVStatusResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, bool joinedGroup, bool replicated,
                ui64 incarnationGuid)
        {
            Record.SetStatus(status);
            Record.SetJoinedGroup(joinedGroup);
            Record.SetReplicated(replicated);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
        }

        TEvVStatusResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
            Y_ABORT_UNLESS(status != NKikimrProto::OK);
            Record.SetStatus(status);
            Record.SetJoinedGroup(false);
            Record.SetReplicated(false);
            Record.MutableVDiskID()->CopyFrom(vdisk);
        }
    };

    struct TEvRegisterPDiskLoadActor: public TEventLocal<TEvRegisterPDiskLoadActor,
            TEvBlobStorage::EvRegisterPDiskLoadActor> {
        TEvRegisterPDiskLoadActor()
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvRegisterPDiskLoadActor";
            str << "}";
            return str.Str();
        }
    };

    struct TEvRegisterPDiskLoadActorResult: public TEventLocal<TEvRegisterPDiskLoadActorResult,
            TEvBlobStorage::EvRegisterPDiskLoadActorResult> {
        NPDisk::TOwnerRound OwnerRound;

        TEvRegisterPDiskLoadActorResult(NPDisk::TOwnerRound ownerRound)
            : OwnerRound(ownerRound)
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvRegisterPDiskLoadActorResult";
            str << " OwnerRound# " << OwnerRound;
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvMonStreamQuery : TEventLocal<TEvMonStreamQuery, EvMonStreamQuery> {
        TString StreamId; // unique stream id provided by the user (guid, possibly)
        TMaybe<ui64> TabletId; // tablet filter
        TMaybe<ui8> Channel; // channel filter

        TEvMonStreamQuery(TString streamId, TMaybe<ui64> tabletId, TMaybe<ui8> channel)
            : StreamId(std::move(streamId))
            , TabletId(tabletId)
            , Channel(channel)
        {}
    };

    struct TEvBlobStorage::TEvMonStreamActorDeathNote : TEventLocal<TEvMonStreamActorDeathNote, EvMonStreamActorDeathNote> {
        TString StreamId;

        TEvMonStreamActorDeathNote(TString streamId)
            : StreamId(std::move(streamId))
        {}
    };

    struct TEvBlobStorage::TEvDropDonor : TEventLocal<TEvDropDonor, EvDropDonor> {
        const ui32 NodeId;
        const ui32 PDiskId;
        const ui32 VSlotId;
        const TVDiskID VDiskId;

        TEvDropDonor(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TVDiskID& vdiskId)
            : NodeId(nodeId)
            , PDiskId(pdiskId)
            , VSlotId(vslotId)
            , VDiskId(vdiskId)
        {}
    };

    struct TEvBlobStorage::TEvBunchOfEvents : TEventLocal<TEvBunchOfEvents, EvBunchOfEvents> {
        std::vector<std::unique_ptr<IEventHandle>> Bunch;

        void Process(IActor *actor) {
            for (auto& ev : Bunch) {
                TAutoPtr<IEventHandle> handle(ev.release());
                actor->Receive(handle);
            }
        }
    };

    struct TEvBlobStorage::TEvAskWardenRestartPDisk : TEventLocal<TEvAskWardenRestartPDisk, EvAskWardenRestartPDisk> {
        const ui32 PDiskId;

        TEvAskWardenRestartPDisk(const ui32& pdiskId)
            : PDiskId(pdiskId)
        {}
    };

    struct TEvBlobStorage::TEvAskRestartVDisk : TEventLocal<TEvAskRestartVDisk, EvAskRestartVDisk> {
        const ui32 PDiskId;
        const TVDiskID VDiskId;

        TEvAskRestartVDisk(
            const ui32 pDiskId,
            const TVDiskID& vDiskId
        )
            : PDiskId(pDiskId)
            , VDiskId(vDiskId)
        {}
    };

    struct TEvBlobStorage::TEvAskWardenRestartPDiskResult : TEventLocal<TEvAskWardenRestartPDiskResult, EvAskWardenRestartPDiskResult> {
        const ui32 PDiskId;
        const NPDisk::TMainKey MainKey;
        const bool RestartAllowed;
        TIntrusivePtr<TPDiskConfig> Config;
        TString Details;

        TEvAskWardenRestartPDiskResult(const ui32 pdiskId, const NPDisk::TMainKey& mainKey, const bool restartAllowed, const TIntrusivePtr<TPDiskConfig>& config,
            TString details = "")
            : PDiskId(pdiskId)
            , MainKey(mainKey)
            , RestartAllowed(restartAllowed)
            , Config(config)
            , Details(details)
        {}
    };

    struct TEvBlobStorage::TEvNotifyWardenPDiskRestarted : TEventLocal<TEvNotifyWardenPDiskRestarted, EvNotifyWardenPDiskRestarted> {
        const ui32 PDiskId;
        NKikimrProto::EReplyStatus Status;

        TEvNotifyWardenPDiskRestarted(const ui32 pdiskId, NKikimrProto::EReplyStatus status = NKikimrProto::EReplyStatus::OK)
            : PDiskId(pdiskId)
            , Status(status)
        {}
    };

    struct TEvBlobStorage::TEvControllerScrubQueryStartQuantum : TEventPB<TEvControllerScrubQueryStartQuantum,
            NKikimrBlobStorage::TEvControllerScrubQueryStartQuantum, EvControllerScrubQueryStartQuantum> {
        TEvControllerScrubQueryStartQuantum() = default;

        TEvControllerScrubQueryStartQuantum(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
            auto *x = Record.MutableVSlotId();
            x->SetNodeId(nodeId);
            x->SetPDiskId(pdiskId);
            x->SetVSlotId(vslotId);
        }
    };

    struct TEvBlobStorage::TEvControllerScrubStartQuantum : TEventPB<TEvControllerScrubStartQuantum,
            NKikimrBlobStorage::TEvControllerScrubStartQuantum, EvControllerScrubStartQuantum> {
        TEvControllerScrubStartQuantum() = default;

        TEvControllerScrubStartQuantum(ui32 nodeId, ui32 pdiskId, ui32 vslotId, std::optional<TString> state) {
            auto *x = Record.MutableVSlotId();
            x->SetNodeId(nodeId);
            x->SetPDiskId(pdiskId);
            x->SetVSlotId(vslotId);
            if (state) {
                Record.SetState(*state);
            }
        }
    };

    struct TEvBlobStorage::TEvControllerScrubQuantumFinished : TEventPB<TEvControllerScrubQuantumFinished,
            NKikimrBlobStorage::TEvControllerScrubQuantumFinished, EvControllerScrubQuantumFinished> {
        TEvControllerScrubQuantumFinished() = default;

        TEvControllerScrubQuantumFinished(const NKikimrBlobStorage::TEvControllerScrubQuantumFinished& m) {
            Record.CopyFrom(m);
        }

        TEvControllerScrubQuantumFinished(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TString& state)
            : TEvControllerScrubQuantumFinished(nodeId, pdiskId, vslotId)
        {
            Record.SetState(state);
        }

        TEvControllerScrubQuantumFinished(ui32 nodeId, ui32 pdiskId, ui32 vslotId, bool success)
            : TEvControllerScrubQuantumFinished(nodeId, pdiskId, vslotId)
        {
            Record.SetSuccess(success);
        }

        TEvControllerScrubQuantumFinished(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
            auto *x = Record.MutableVSlotId();
            x->SetNodeId(nodeId);
            x->SetPDiskId(pdiskId);
            x->SetVSlotId(vslotId);
        }
    };

    struct TEvBlobStorage::TEvControllerScrubReportQuantumInProgress : TEventPB<TEvControllerScrubReportQuantumInProgress,
            NKikimrBlobStorage::TEvControllerScrubReportQuantumInProgress, EvControllerScrubReportQuantumInProgress> {
        TEvControllerScrubReportQuantumInProgress() = default;

        TEvControllerScrubReportQuantumInProgress(ui32 nodeId, ui32 pdiskId, ui32 vslotId) {
            auto *x = Record.MutableVSlotId();
            x->SetNodeId(nodeId);
            x->SetPDiskId(pdiskId);
            x->SetVSlotId(vslotId);
        }
    };

    struct TEvBlobStorage::TEvControllerGroupDecommittedNotify : TEventPB<TEvControllerGroupDecommittedNotify,
            NKikimrBlobStorage::TEvControllerGroupDecommittedNotify, EvControllerGroupDecommittedNotify> {
        TEvControllerGroupDecommittedNotify() = default;

        TEvControllerGroupDecommittedNotify(ui32 groupId) {
            Record.SetGroupId(groupId);
        }
    };

    struct TEvBlobStorage::TEvControllerGroupDecommittedResponse : TEventPB<TEvControllerGroupDecommittedResponse,
            NKikimrBlobStorage::TEvControllerGroupDecommittedResponse, EvControllerGroupDecommittedResponse> {
        TEvControllerGroupDecommittedResponse() = default;

        TEvControllerGroupDecommittedResponse(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }
    };

    struct TEvBlobStorage::TEvControllerGroupMetricsExchange : TEventPB<TEvControllerGroupMetricsExchange,
            NKikimrBlobStorage::TEvControllerGroupMetricsExchange, EvControllerGroupMetricsExchange>
    {};

    struct TEvBlobStorage::TEvPutVDiskToReadOnly : TEventLocal<TEvPutVDiskToReadOnly, EvPutVDiskToReadOnly> {
        const TVDiskID VDiskId;

        TEvPutVDiskToReadOnly(TVDiskID vDiskId)
            : VDiskId(std::move(vDiskId))
        {}
    };

    struct TEvNodeWardenQueryGroupInfo : TEventPB<TEvNodeWardenQueryGroupInfo, NKikimrBlobStorage::TEvNodeWardenQueryGroupInfo,
            TEvBlobStorage::EvNodeWardenQueryGroupInfo> {
        TEvNodeWardenQueryGroupInfo() = default;

        TEvNodeWardenQueryGroupInfo(ui32 groupId) {
            Record.SetGroupId(groupId);
        }
    };

    struct TEvNodeWardenGroupInfo : TEventPB<TEvNodeWardenGroupInfo, NKikimrBlobStorage::TEvNodeWardenGroupInfo,
            TEvBlobStorage::EvNodeWardenGroupInfo> {
        TEvNodeWardenGroupInfo() = default;

        TEvNodeWardenGroupInfo(const NKikimrBlobStorage::TGroupInfo& group) {
            Record.MutableGroup()->CopyFrom(group);
        }
    };

    struct TEvStatusUpdate : TEventLocal<TEvStatusUpdate, TEvBlobStorage::EvStatusUpdate> {
        ui32 NodeId;
        ui32 PDiskId;
        ui32 VSlotId;
        NKikimrBlobStorage::EVDiskStatus Status;
        bool OnlyPhantomsRemain;

        TEvStatusUpdate(ui32 nodeId, ui32 pdiskId, ui32 vslotId, NKikimrBlobStorage::EVDiskStatus status,
                bool onlyPhantomsRemain)
            : NodeId(nodeId)
            , PDiskId(pdiskId)
            , VSlotId(vslotId)
            , Status(status)
            , OnlyPhantomsRemain(onlyPhantomsRemain)
        {}
    };

    struct TEvNodeWardenQueryStorageConfig
        : TEventLocal<TEvNodeWardenQueryStorageConfig, TEvBlobStorage::EvNodeWardenQueryStorageConfig>
    {
        bool Subscribe = false;

        TEvNodeWardenQueryStorageConfig(bool subscribe)
            : Subscribe(subscribe)
        {}
    };

    struct TEvNodeWardenStorageConfig
        : TEventLocal<TEvNodeWardenStorageConfig, TEvBlobStorage::EvNodeWardenStorageConfig>
    {
        std::unique_ptr<NKikimrBlobStorage::TStorageConfig> Config;
        std::unique_ptr<NKikimrBlobStorage::TStorageConfig> ProposedConfig;

        TEvNodeWardenStorageConfig(const NKikimrBlobStorage::TStorageConfig& config,
                const NKikimrBlobStorage::TStorageConfig *proposedConfig);
        ~TEvNodeWardenStorageConfig();
    };

} // NKikimr
