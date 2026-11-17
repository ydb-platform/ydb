#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/common/heartbeat.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

class TCdcAggregatorHeartbeatPublication {
public:
    explicit TCdcAggregatorHeartbeatPublication(const TVector<ui32>& partitionIds);

    const std::optional<NPQ::THeartbeat>& GetHeartbeat() const {
        return Heartbeat;
    }

    bool HasFailedEmits() const {
        return !FailedPartitionIds.empty();
    }

    void SetHeartbeat(const NPQ::THeartbeat& heartbeat);
    TVector<ui32> TakePartitionsToEmit();
    bool CompleteEmit(ui32 partitionId, const TRowVersion& emittedVersion);
    void FailEmit(ui32 partitionId);
    void RetryFailedEmits();

private:
    std::optional<NPQ::THeartbeat> Heartbeat;
    THashMap<ui32, TRowVersion> EmittedVersionByPartitionId;
    THashMap<ui32, TRowVersion> InFlightVersionByPartitionId;
    THashSet<ui32> FailedPartitionIds;
};

} // namespace NKikimr::NCdcAggregator
