#pragma once

#include "change_exchange_helpers.h"
#include "change_record_cdc_serializer.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/persqueue/common/heartbeat.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/util/backoff.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NDataShard {

class TCdcAggregatedHeartbeats {
public:
    using TRecordInfo = NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo;
    using TEvWriteHeartbeatResult = NCdcAggregator::TEvCdcAggregatorService::TEvWriteHeartbeatResult;

    explicit TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer, const TDataShardId& dataShard);

    void OnRecordsEnqueued(const TVector<TRecordInfo>& records);
    TVector<ui64> OnRecordsReceived(TVector<NChangeExchange::IChangeRecord::TPtr>& records);
    void OnRecordsRemoved(const TVector<ui64>& orders);
    void OnStreamResolved(ui64 aggregatorTabletId);
    void OnWriteResult(const TEvWriteHeartbeatResult& result, ui64 cookie);
    void OnUndelivered(ui64 cookie);
    void RemoveAll();

private:
    void SendIfReady();
    void Send(TDuration delay);
    void RemoveHeld(TMap<ui64, NPQ::THeartbeat>::iterator end);

    THolder<IChangeRecordSerializer> Serializer;
    const TDataShardId DataShard;

    TSet<ui64> Undelivered;
    TMap<ui64, NPQ::THeartbeat> Held;
    std::optional<ui64> Sent;

    ui64 AggregatorTabletId = 0;
    ui64 Cookie = 0;
    TBackoff Backoff = TBackoff(TDuration::MilliSeconds(100), TDuration::Seconds(10));
};

} // namespace NKikimr::NDataShard
