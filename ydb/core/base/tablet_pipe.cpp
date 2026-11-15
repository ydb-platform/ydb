#include "tablet_pipe.h"

namespace NKikimr {

TEvTabletPipe::TEvConnect::TEvConnect(ui64 tabletId, const TActorId& clientId, ui32 features)
{
    Record.SetTabletId(tabletId);
    ActorIdToProto(clientId, Record.MutableClientId());
    if (features) {
        Record.SetFeatures(features);
    }
}

TEvTabletPipe::TEvConnectResult::TEvConnectResult(NKikimrProto::EReplyStatus status, ui64 tabletId,
        const TActorId& clientId, const TActorId& serverId,
        bool leader, ui32 generation,
        TString&& versionInfo)
{
    Record.SetStatus(status);
    Record.SetTabletId(tabletId);
    ActorIdToProto(clientId, Record.MutableClientId());
    ActorIdToProto(serverId, Record.MutableServerId());
    Record.SetLeader(leader);
    Record.SetSupportsDataInPayload(true);
    Record.SetGeneration(generation);
    if (!versionInfo.empty()) {
        Record.SetVersionInfo(std::move(versionInfo));
    }
}

TEvTabletPipe::TEvPush::TEvPush(ui64 tabletId, ui32 type, const TActorId& sender, const TIntrusivePtr<TEventSerializedData>& buffer,
    ui64 cookie, const TEventSerializationInfo& serializationInfo, bool supportsDataInPayload)
{
    Record.SetTabletId(tabletId);
    Record.SetType(type);
    ActorIdToProto(sender, Record.MutableSender());
    if (supportsDataInPayload) {
        AddPayload(buffer->GetRope());
    } else {
        Record.SetBuffer(buffer->GetString());
    }
    Record.SetCookie(cookie);
    Record.SetExtendedFormat(serializationInfo.IsExtendedFormat);
}

TEvTabletPipe::TEvPeerClosed::TEvPeerClosed(ui64 tabletId, const TActorId& clientId, const TActorId& serverId)
{
    Record.SetTabletId(tabletId);
    ActorIdToProto(clientId, Record.MutableClientId());
    ActorIdToProto(serverId, Record.MutableServerId());
}

TEvTabletPipe::TEvPeerShutdown::TEvPeerShutdown(ui64 tabletId, const TActorId& clientId, const TActorId& serverId, ui64 maxForwardedSeqNo)
{
    Record.SetTabletId(tabletId);
    ActorIdToProto(clientId, Record.MutableClientId());
    ActorIdToProto(serverId, Record.MutableServerId());
    Record.SetMaxForwardedSeqNo(maxForwardedSeqNo);
}

ui64 TEvTabletPipe::TEvPeerShutdown::GetMaxForwardedSeqNo() const {
    if (Record.HasMaxForwardedSeqNo()) {
        return Record.GetMaxForwardedSeqNo();
    } else {
        return Max<ui64>();
    }
}

TString TEvTabletPipe::TEvClientConnected::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " TabletId: " << TabletId
        << " Status: " << Status
        << " ServerId: " << ServerId
        << " Leader: " << Leader
        << " Dead: " << Dead
        << " Generation: " << Generation
        << " VersionInfo: " << VersionInfo
    << " }";
}

TString TEvTabletPipe::TEvClientDestroyed::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " TabletId: " << TabletId
        << " ClientId: " << ClientId
        << " ServerId: " << ServerId
    << " }";
}

} // namespace NKikimr

namespace NKikimr::NTabletPipe {

TClientRetryPolicy TClientRetryPolicy::WithoutRetries() {
    return {
        .RetryLimitCount = {},
        .MinRetryTime = {},
        .MaxRetryTime = {},
        .BackoffMultiplier = {},
        .DoFirstRetryInstantly = {},
        };
}

} // namespace NKikimr::NTabletPipe
