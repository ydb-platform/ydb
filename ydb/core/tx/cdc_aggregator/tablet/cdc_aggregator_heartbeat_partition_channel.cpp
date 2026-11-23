#include "cdc_aggregator_heartbeat_partition_channel.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <algorithm>
#include <utility>

namespace NKikimr::NCdcAggregator {

namespace {

THolder<TEvPersQueue::TEvRequest> MakeHeartbeatRequest(ui32 partitionId, const NPQ::THeartbeat& heartbeat) {
    auto request = MakeHolder<TEvPersQueue::TEvRequest>();
    auto& partitionRequest = *request->Record.MutablePartitionRequest();
    partitionRequest.SetPartition(partitionId);
    partitionRequest.SetCookie(partitionId);
    heartbeat.Serialize(*partitionRequest.MutableCmdEmitHeartbeat()->MutableHeartbeat());
    return request;
}

std::optional<TRowVersion> ParseConfirmedVersion(const NKikimrClient::TResponse& response) {
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK
        || response.GetErrorCode() != NPersQueue::NErrorCode::OK
        || !response.GetPartitionResponse().HasCmdEmitHeartbeatResult())
    {
        return std::nullopt;
    }

    const auto& result = response.GetPartitionResponse().GetCmdEmitHeartbeatResult();
    return TRowVersion(result.GetStep(), result.GetTxId());
}

} // namespace

TCdcAggregatorHeartbeatPartitionChannel::TCdcAggregatorHeartbeatPartitionChannel(ui32 partitionId, TCdcAggregatorHeartbeatTabletPipe::TPtr tabletPipe)
    : PartitionId(partitionId)
    , TabletPipe(std::move(tabletPipe))
{
}

void TCdcAggregatorHeartbeatPartitionChannel::SendHeartbeat(const NPQ::THeartbeat& latestHeartbeat) {
    const bool waitingForResponse = SentVersion.has_value();
    const bool alreadyConfirmed = latestHeartbeat.Version <= ConfirmedVersion;
    if (waitingForResponse || alreadyConfirmed) {
        return;
    }
    TabletPipe->Send(MakeHeartbeatRequest(PartitionId, latestHeartbeat));
    SentVersion = latestHeartbeat.Version;
}

TCdcAggregatorHeartbeatPartitionChannel::EStatus TCdcAggregatorHeartbeatPartitionChannel::HandleResponse(
        const NKikimrClient::TResponse& response, const NPQ::THeartbeat& latestHeartbeat)
{
    const auto sentVersion = std::exchange(SentVersion, std::nullopt);
    const auto confirmedVersion = ParseConfirmedVersion(response);
    if (!confirmedVersion) {
        return EStatus::Fail;
    }
    ConfirmedVersion = std::max(ConfirmedVersion, *confirmedVersion);
    if (sentVersion && ConfirmedVersion < *sentVersion) {
        return EStatus::Fail;
    }

    SendHeartbeat(latestHeartbeat);
    return EStatus::Ok;
}

bool TCdcAggregatorHeartbeatPartitionChannel::ForgetSentHeartbeat() {
    return std::exchange(SentVersion, std::nullopt).has_value();
}

} // namespace NKikimr::NCdcAggregator
