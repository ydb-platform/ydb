#pragma once

#include "change_record_cdc_serializer.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>
#include <utility>

namespace NKikimr::NDataShard {

class TCdcAggregatedHeartbeats {
    using IChangeRecord = NChangeExchange::IChangeRecord;
    using TRecordInfo = NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo;

public:
    explicit TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, ui64 sourceDataShardId);

    void SetAggregator(ui64 tabletId);
    void RegisterPendingRecordIds(const TVector<TRecordInfo>& records);
    void RemovePendingRecordIds(const TVector<ui64>& orders);
    std::pair<TVector<IChangeRecord::TPtr>, TVector<ui64>> BufferHeartbeats(TVector<IChangeRecord::TPtr> records);
    TVector<ui64> HandleCdcAggregatorServiceReply(TAutoPtr<IEventHandle>& ev);
    TVector<ui64> Clear();

private:
    void SendReadyHeartbeat();
    TVector<ui64> CompleteInFlight();
    TVector<ui64> RetryInFlight();
    void Send(TDuration delay);

    THolder<IChangeRecordSerializer> Serializer;
    const ui64 SourceDataShardId;
    ui64 AggregatorTabletId = 0;

    TSet<ui64> PendingRecords;
    TMap<ui64, IChangeRecord::TPtr> PendingHeartbeats;
    std::optional<ui64> InFlight;
    TBackoff Backoff = TBackoff(TDuration::MilliSeconds(100), TDuration::Seconds(10));
};

} // namespace NKikimr::NDataShard
