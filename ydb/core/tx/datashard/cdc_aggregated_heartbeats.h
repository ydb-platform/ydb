#pragma once

#include "change_record_cdc_serializer.h"

#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/change_exchange/change_record.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NDataShard {

class TCdcAggregatedHeartbeats {
public:
    using TRecordInfo = NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo;

    struct TToSend {
        NPQ::THeartbeat Heartbeat;
        TVector<ui64> Covered;
    };

    explicit TCdcAggregatedHeartbeats(THolder<IChangeRecordSerializer> serializer);

    void OnRecordsEnqueued(const TVector<TRecordInfo>& records);
    void OnRecordsRemoved(const TVector<ui64>& orders);
    TVector<ui64> ExtractHeartbeats(TVector<NChangeExchange::IChangeRecord::TPtr>& records);

    std::optional<TToSend> TakeHeartbeatToSend();
    const NPQ::THeartbeat* GetSentHeartbeat() const;
    TVector<ui64> CompleteSentHeartbeat();
    TVector<ui64> Reset();

private:
    THolder<IChangeRecordSerializer> Serializer;
    TSet<ui64> Undelivered;
    TMap<ui64, NPQ::THeartbeat> Held;
    std::optional<ui64> Sent;
};

} // namespace NKikimr::NDataShard
