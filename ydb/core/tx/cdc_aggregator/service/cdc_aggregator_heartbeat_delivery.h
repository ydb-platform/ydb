#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorHeartbeatDeliveryItem {
    ui64 SourceDataShardId = 0;
    NPQ::THeartbeat Heartbeat;
};

struct TCdcAggregatorHeartbeatDeliveryResult {
    THashMap<ui64, TRowVersion> WrittenVersions;
    THashMap<ui64, TString> RejectionErrors;
};

struct TCdcAggregatorHeartbeatDeliveryReply {
    enum class EStatus {
        Written,
        Retry,
        Rejected,
    };

    ui64 SourceDataShardId = 0;
    EStatus Status = EStatus::Retry;
    TRowVersion WrittenVersion = TRowVersion::Min();
    TString Error;
};

class TCdcAggregatorHeartbeatDelivery {
public:
    explicit TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize);

    void AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat);
    TVector<TCdcAggregatorHeartbeatDeliveryItem> TakeBatch();
    TVector<TCdcAggregatorHeartbeatDeliveryReply> CompleteBatch(const TCdcAggregatorHeartbeatDeliveryResult& result);
    TVector<TCdcAggregatorHeartbeatDeliveryReply> FailBatch(const TString& error);

    bool HasBatchToSend() const {
        return InFlight.empty() && !PendingSourceIds.empty();
    }

    bool IsIdle() const {
        return Pending.empty() && InFlight.empty();
    }

private:
    const ui32 MaxBatchSize;

    THashMap<ui64, NPQ::THeartbeat> Pending;
    TDeque<ui64> PendingSourceIds;
    TVector<ui64> InFlight;
};

} // namespace NKikimr::NCdcAggregator
