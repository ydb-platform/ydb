#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/library/actors/core/actorid.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorHeartbeatDeliveryWaiter {
    TActorId Sender;
    ui64 Cookie = 0;
};

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

    TCdcAggregatorHeartbeatDeliveryWaiter Waiter;
    EStatus Status = EStatus::Retry;
    TRowVersion WrittenVersion = TRowVersion::Min();
    TString Error;
};

class TCdcAggregatorHeartbeatDelivery {
public:
    TCdcAggregatorHeartbeatDelivery(ui32 maxBatchSize, ui32 maxWaiters);

    bool AddHeartbeat(ui64 sourceDataShardId, const NPQ::THeartbeat& heartbeat, const TCdcAggregatorHeartbeatDeliveryWaiter& waiter);
    TVector<TCdcAggregatorHeartbeatDeliveryItem> TakeBatch();
    TVector<TCdcAggregatorHeartbeatDeliveryReply> CompleteBatch(const TCdcAggregatorHeartbeatDeliveryResult& result);
    TVector<TCdcAggregatorHeartbeatDeliveryReply> FailBatch(const TString& error);
    TVector<TCdcAggregatorHeartbeatDeliveryReply> RetryAllWaiters(const TString& error);

    bool HasBatchToSend() const {
        return InFlight.empty() && !PendingSourceIds.empty();
    }

    bool IsIdle() const {
        return Sources.empty();
    }

    ui32 GetWaitersCount() const {
        return WaitersCount;
    }

private:
    struct TVersionedWaiter {
        TCdcAggregatorHeartbeatDeliveryWaiter Waiter;
        TRowVersion Version;
    };

    struct TSource {
        std::optional<NPQ::THeartbeat> Pending;
        TVector<TVersionedWaiter> Waiters;
    };

    TVector<TCdcAggregatorHeartbeatDeliveryWaiter> ExtractWaiters(TSource& source, const TRowVersion& upTo);

    const ui32 MaxBatchSize;
    const ui32 MaxWaiters;

    THashMap<ui64, TSource> Sources;
    TDeque<ui64> PendingSourceIds;
    THashMap<ui64, TRowVersion> InFlight;
    ui32 WaitersCount = 0;
};

} // namespace NKikimr::NCdcAggregator
