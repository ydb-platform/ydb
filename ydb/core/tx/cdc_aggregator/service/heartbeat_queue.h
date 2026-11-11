#pragma once

#include <ydb/core/tx/cdc_aggregator/public/events.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

class THeartbeatQueue {
public:
    using EStatus = TEvCdcAggregatorService::TEvWriteHeartbeatResult::EStatus;

    struct TReply {
        TActorId Sender;
        ui64 Cookie = 0;
        EStatus Status = EStatus::Retry;
        TRowVersion WrittenVersion = TRowVersion::Min();
        TString Error;
    };

    THeartbeatQueue(ui32 maxBatchSize, ui32 maxWaiters);

    bool Enqueue(ui64 sourceId, const NPQ::THeartbeat& heartbeat, const TActorId& sender, ui64 cookie);
    bool CanSend() const;
    bool IsIdle() const;
    ui32 GetWaitersCount() const;

    THolder<TEvCdcAggregator::TEvWriteHeartbeats> NextBatch();
    TVector<TReply> OnResult(const NKikimrCdcAggregator::TEvWriteHeartbeatsResult& record);
    TVector<TReply> Fail(const TString& error);

private:
    struct TWaiter {
        TActorId Sender;
        ui64 Cookie = 0;
        TRowVersion Version;
    };

    struct TSource {
        std::optional<NPQ::THeartbeat> Pending;
        TVector<TWaiter> Waiters;
    };

    void Complete(TSource& source, const TRowVersion& upTo, EStatus status,
        const TRowVersion& writtenVersion, const TString& error, TVector<TReply>& replies);
    void CompleteInFlight(EStatus status, const TString& error, TVector<TReply>& replies);

private:
    const ui32 MaxBatchSize;
    const ui32 MaxWaiters;

    THashMap<ui64, TSource> Sources;
    TDeque<ui64> Queue;
    THashMap<ui64, TRowVersion> InFlight;
    ui32 WaitersCount = 0;
};

} // namespace NKikimr::NCdcAggregator
