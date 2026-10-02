#pragma once

#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>

#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <vector>

namespace NYql::NDq {

constexpr ui64 GetExpectedTopicReadTasks(
    ui64 partitionsCount,
    ui64 maxPartitions,
    bool groupPartitions)
{
    constexpr ui64 AveragePartitionsPerTask = 5;
    return groupPartitions
        ? (partitionsCount + AveragePartitionsPerTask - 1) / AveragePartitionsPerTask
        : Min(maxPartitions, partitionsCount);
}

[[nodiscard]] std::vector<NPq::NProto::TDqReadTaskParams> ExtractReadTaskParams(
   const THashMap<TString, TString>& taskParams, // partitions are here in dq
   const TVector<TString>& readRanges            // partitions are here in kqp
);

[[nodiscard]] std::vector<TPartitionKey> GetPartitionsToRead(
    const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams,
    const std::vector<TPartitionKey>& federatedClusters
);

} // namespace NYql::NDq
