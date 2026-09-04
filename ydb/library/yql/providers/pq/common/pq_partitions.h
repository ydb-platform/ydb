#pragma once

#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>

#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <vector>

namespace NYql::NDq {

[[nodiscard]] std::vector<NPq::NProto::TDqReadTaskParams> ExtractReadTaskParams(
    const THashMap<TString, TString>& taskParams, // partitions are here in dq
    const TVector<TString>& readRanges            // partitions are here in kqp
);

[[nodiscard]] std::vector<TPartitionKey> GetPartitionsToRead(
    const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams,
    const std::vector<TPartitionKey>& federatedClusters
);

} // namespace NYql::NDq
