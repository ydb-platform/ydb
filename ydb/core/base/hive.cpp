#include "hive.h"
#include <util/stream/str.h>

namespace NKikimr::TEvHive {

TString TEvBootTablet::ToString() const {
    TStringStream str;
    str << "{EvBootTablet TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TString TEvBootTabletReply::ToString() const {
    TStringStream str;
    str << "{EvBootTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << " Msg: " << Record.GetStatusMsg();
    str << "}";
    return str.Str();
}

TEvCreateTablet::TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
                TTabletTypes::EType tabletType,
                const TChannelsBindings& bindedChannels) {
    Record.SetOwner(ownerId);
    Record.SetOwnerIdx(ownerIdx);
    Record.SetTabletType(tabletType);
    for (auto& channel : bindedChannels) {
        *Record.AddBindedChannels() = channel;
    }
}

TEvCreateTablet::TEvCreateTablet(ui64 ownerId,
                ui64 ownerIdx,
                TTabletTypes::EType tabletType) {
    Record.SetOwner(ownerId);
    Record.SetOwnerIdx(ownerIdx);
    Record.SetTabletType(tabletType);
}

TEvCreateTablet::TEvCreateTablet(ui64 ownerId, ui64 ownerIdx,
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

TString TEvCreateTablet::ToString() const {
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

TEvCreateTabletReply::TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx) {
    Record.SetStatus(status);
    Record.SetOwner(ownerId);
    Record.SetOwnerIdx(ownerIdx);
}

TEvCreateTabletReply::TEvCreateTabletReply(NKikimrProto::EReplyStatus status, ui64 ownerId, ui64 ownerIdx, ui64 tabletId)
    : TEvCreateTabletReply(status, ownerId, ownerIdx)
{
    if (tabletId != 0)
        Record.SetTabletID(tabletId);
}

TEvCreateTabletReply::TEvCreateTabletReply(
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

TString TEvCreateTabletReply::ToString() const {
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

TEvTabletCreationResult::TEvTabletCreationResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    if (tabletId != 0) {
        Record.SetTabletID(tabletId);
    }
}

TString TEvTabletCreationResult::ToString() const {
    TStringStream str;
    str << "{EvTabletCreationResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    if (Record.HasTabletID()) {
        str << " TabletID: " << Record.GetTabletID();
    }
    str << "}";
    return str.Str();
}

TEvStopTablet::TEvStopTablet(ui64 tabletId, const TActorId &actorToNotify) {
    Record.SetTabletID(tabletId);
    ActorIdToProto(actorToNotify, Record.MutableActorToNotify());
}

TString TEvStopTablet::ToString() const {
    TStringStream str;
    str << "{EvStopTablet TabletID: " << Record.GetTabletID();
    if (Record.HasActorToNotify()) {
        str << " ActorToNotify: " << ActorIdFromProto(Record.GetActorToNotify()).ToString();
    }
    str << "}";
    return str.Str();
}

TEvStopTabletResult::TEvStopTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    Record.SetTabletID(tabletId);
}

TString TEvStopTabletResult::ToString() const {
    TStringStream str;
    str << "{EvStopTabletResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus());
    str << " TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvResumeTabletResult::TEvResumeTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    Record.SetTabletID(tabletId);
}

TString TEvResumeTabletResult::ToString() const {
    TStringStream str;
    str << "{EvResumeTabletResult Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus());
    str << " TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvAdoptTablet::TEvAdoptTablet(ui64 tabletId, ui64 prevOwner, ui64 prevOwnerIdx, TTabletTypes::EType tabletType, ui64 owner, ui64 ownerIdx) {
    Record.SetTabletID(tabletId);
    Record.SetPrevOwner(prevOwner);
    Record.SetPrevOwnerIdx(prevOwnerIdx);
    Record.SetTabletType(tabletType);
    Record.SetOwner(owner);
    Record.SetOwnerIdx(ownerIdx);
}

TString TEvAdoptTablet::ToString() const {
    TStringStream str;
    str << "{TEvAdoptTablet TabletID: " << Record.GetTabletID();
    str << " PrevOwner: " << Record.GetPrevOwner();
    str << " PrevOwnerIdx: " << Record.GetPrevOwnerIdx();
    str << " Owner: " << Record.GetOwner();
    str << " OwnerIdx: " << Record.GetOwnerIdx();
    str << "}";
    return str.Str();
}

TEvAdoptTabletReply::TEvAdoptTabletReply(NKikimrProto::EReplyStatus status, ui64 tablet, ui64 owner, ui64 ownerIdx, const TString explain, ui64 origin) {
    Record.SetStatus(status);
    Record.SetTabletID(tablet);
    Record.SetOwner(owner);
    Record.SetOwnerIdx(ownerIdx);
    Record.SetExplain(explain);
    Record.SetOrigin(origin);
}

TString TEvAdoptTabletReply::ToString() const {
    TStringStream str;
    str << "{TEvAdoptTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << " TabletID: " << Record.GetTabletID();
    str << " Explain: " << Record.GetExplain();
    str << "}";
    return str.Str();
}

TString TEvReconfigureTablet::ToString() const {
    TStringStream str;
    str << "{EvReconfigureTablet TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvReconfigureTabletReply::TEvReconfigureTabletReply(NKikimrProto::EReplyStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    Record.SetTabletID(tabletId);
}

TString TEvReconfigureTabletReply::ToString() const {
    TStringStream str;
    str << "{EvReconfigureTabletReply Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << " TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvDeleteTablet::TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 txId) {
    Record.SetShardOwnerId(ownerId);
    Record.AddShardLocalIdx(ownerIdx);
    Record.SetTxId_Deprecated(txId);
}

TEvDeleteTablet::TEvDeleteTablet(ui64 ownerId, ui64 ownerIdx, ui64 tabletId, ui64 txId) {
    Record.SetShardOwnerId(ownerId);
    Record.AddShardLocalIdx(ownerIdx);
    Record.AddTabletID(tabletId);
    Record.SetTxId_Deprecated(txId);
}

TEvDeleteTabletReply::TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 txId, ui64 ownerId, const TVector<ui64>& localIdxs) {
    Record.SetStatus(status);
    Record.SetOrigin(hiveID);
    Record.SetTxId_Deprecated(txId);
    Record.SetShardOwnerId(ownerId);
    for (auto& idx: localIdxs) {
        Record.AddShardLocalIdx(idx);
    }
}

TEvDeleteTabletReply::TEvDeleteTabletReply(NKikimrProto::EReplyStatus status, ui64 hiveID, const NKikimrHive::TEvDeleteTablet& request) {
    Record.SetStatus(status);
    Record.SetOrigin(hiveID);
    Record.SetTxId_Deprecated(request.GetTxId_Deprecated());
    Record.SetShardOwnerId(request.GetShardOwnerId());
    for (auto idx : request.GetShardLocalIdx()) {
        Record.AddShardLocalIdx(idx);
    }
}

TEvDeleteOwnerTablets::TEvDeleteOwnerTablets(ui64 ownerId, ui64 txId) {
    Record.SetOwner(ownerId);
    Record.SetTxId(txId);
}

TEvDeleteOwnerTabletsReply::TEvDeleteOwnerTabletsReply(NKikimrProto::EReplyStatus status, ui64 hiveID, ui64 ownerId, ui64 txId) {
    Record.SetStatus(status);
    Record.SetOwner(ownerId);
    Record.SetTxId(txId);
    Record.SetOrigin(hiveID);
}

TString TEvLookupChannelInfo::ToString() const {
    TStringStream str;
    str << "{EvLookupChannelInfo TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvChannelInfo::TEvChannelInfo(NKikimrProto::EReplyStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    Record.SetTabletID(tabletId);
}

TString TEvChannelInfo::ToString() const {
    TStringStream str;
    str << "{EvChannelInfo Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << " TabletID: " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvRequestHiveInfo::TEvRequestHiveInfo(TRequestHiveInfoInitializer initializer) {
    Record.SetTabletID(initializer.TabletId);
    Record.SetReturnFollowers(initializer.ReturnFollowers);
    Record.SetReturnMetrics(initializer.ReturnMetrics);
    Record.SetReturnChannelHistory(initializer.ReturnChannelHistory);
}

TEvRequestHiveInfo::TEvRequestHiveInfo(ui64 tabletId, bool returnFollowers) {
    Record.SetTabletID(tabletId);
    Record.SetReturnFollowers(returnFollowers);
}

TEvLookupTablet::TEvLookupTablet(ui64 ownerId, ui64 ownerIdx) {
    Record.SetOwner(ownerId);
    Record.SetOwnerIdx(ownerIdx);
}

TEvGetTabletStorageInfoResult::TEvGetTabletStorageInfoResult(ui64 tabletId, NKikimrProto::EReplyStatus status) {
    Record.SetTabletID(tabletId);
    Record.SetStatus(status);
}

TEvGetTabletStorageInfoResult::TEvGetTabletStorageInfoResult(ui64 tabletId, const TTabletStorageInfo& info) {
    Record.SetTabletID(tabletId);
    Record.SetStatus(NKikimrProto::OK);
    TabletStorageInfoToProto(info, Record.MutableInfo());
}

TEvLockTabletExecutionResult::TEvLockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage) {
    Record.SetTabletID(tabletId);
    Record.SetStatus(status);
    Record.SetStatusMessage(statusMessage);
}

TString TEvLockTabletExecutionResult::ToString() const {
    TStringStream str;
    str << ToStringHeader() << "{Status: " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
    str << " TabletID: " << Record.GetTabletID();
    str << " Message: " << Record.GetStatusMessage();
    str << "}";
    return str.Str();
}

TEvLockTabletExecutionLost::TEvLockTabletExecutionLost(ui64 tabletId, NKikimrHive::ELockLostReason reason) {
    Record.SetTabletID(tabletId);
    Record.SetReason(reason);
}

TEvUnlockTabletExecutionResult::TEvUnlockTabletExecutionResult(ui64 tabletId, NKikimrProto::EReplyStatus status, const TString& statusMessage) {
    Record.SetTabletID(tabletId);
    Record.SetStatus(status);
    Record.SetStatusMessage(statusMessage);
}

TEvRequestTabletIdSequence::TEvRequestTabletIdSequence(ui64 ownerId, ui64 ownerIdx, size_t size) {
    Record.MutableOwner()->SetOwner(ownerId);
    Record.MutableOwner()->SetOwnerIdx(ownerIdx);
    Record.SetSize(size);
}

TEvQueryMigrationReply::TEvQueryMigrationReply(NKikimrHive::EMigrationState state, i32 progress) {
    Record.SetMigrationState(state);
    Record.SetMigrationProgress(progress);
}

TEvSetDown::TEvSetDown(ui32 nodeId, bool down) {
    Record.SetNodeId(nodeId);
    Record.SetDown(down);
}

} // namespace NKikimr::TEvHive
