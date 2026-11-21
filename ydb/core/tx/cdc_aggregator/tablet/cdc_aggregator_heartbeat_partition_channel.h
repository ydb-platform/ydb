#pragma once

#include "cdc_aggregator_heartbeat_tablet_pipe.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/protos/msgbus.pb.h>

#include <util/generic/ptr.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorHeartbeatPartitionChannel : public TSimpleRefCount<TCdcAggregatorHeartbeatPartitionChannel> {
public:
    using TPtr = TIntrusivePtr<TCdcAggregatorHeartbeatPartitionChannel>;

    enum class EStatus {
        Ok,
        Fail,
    };

    TCdcAggregatorHeartbeatPartitionChannel(ui32 partitionId, TCdcAggregatorHeartbeatTabletPipe::TPtr tabletPipe);

    ui64 GetPersQueueTabletId() const {
        return TabletPipe->GetTabletId();
    }

    void SendHeartbeat(const NPQ::THeartbeat& latestHeartbeat);
    [[nodiscard]] EStatus HandleResponse(const NKikimrClient::TResponse& response, const NPQ::THeartbeat& latestHeartbeat);
    [[nodiscard]] bool ForgetSentHeartbeat();

private:
    const ui32 PartitionId;
    const TCdcAggregatorHeartbeatTabletPipe::TPtr TabletPipe;
    TRowVersion ConfirmedVersion = TRowVersion::Min();
    std::optional<TRowVersion> SentVersion;
};

} // namespace NKikimr::NCdcAggregator
