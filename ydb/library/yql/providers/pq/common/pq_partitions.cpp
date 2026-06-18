#include "pq_partitions.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NYql::NDq {

namespace {

std::vector<TPartitionKey> GetPartitionsToRead(const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams, const TMaybe<TPartitionKey>& federatedCluster) {
    const auto cluster = federatedCluster.Transform(std::bind(&TPartitionKey::Cluster, std::placeholders::_1)).GetOrElse("");
    const auto maybeTopicPartitionsCount = federatedCluster.Transform(std::bind(&TPartitionKey::PartitionId, std::placeholders::_1));

    std::vector<TPartitionKey> result;
    for (const auto& readTaskParams : readTaskParams) {
        for (const auto& partitioningParams : readTaskParams.GetPartitioningParams()) {
            auto topicPartitionsCount = maybeTopicPartitionsCount.GetOrElse(partitioningParams.GetTopicPartitionsCount());
            if (!topicPartitionsCount) {
                topicPartitionsCount = partitioningParams.GetTopicPartitionsCount();
            }
            for (
                auto pk = partitioningParams.GetEachTopicPartitionGroupId();
                pk < topicPartitionsCount;
                pk += partitioningParams.GetDqPartitionsCount()
            ) {
                result.emplace_back(cluster, pk); // 0-based in topic API
            }
        }
    }
    return result;
}

} // anonymous namespace

std::vector<NPq::NProto::TDqReadTaskParams> ExtractReadTaskParams(
    const THashMap<TString, TString>& taskParams, // partitions are here in dq
    const TVector<TString>& readRanges            // partitions are here in kqp
) {
    std::vector<NPq::NProto::TDqReadTaskParams> result;
    if (!readRanges.empty()) {
        for (const auto& readRange : readRanges) {
            NPq::NProto::TDqReadTaskParams params;
            YQL_ENSURE(params.ParseFromString(readRange), "Failed to parse NPq::NProto::TDqReadTaskParams from ReadRanges");
            result.emplace_back(std::move(params));
        }
    } else {
        auto it = taskParams.find("pq");
        YQL_ENSURE(it != taskParams.end(), "Failed to get DqPqRead task params");
        const auto& taskParam = it->second;

        NPq::NProto::TDqReadTaskParams params;
        YQL_ENSURE(params.ParseFromString(taskParam), "Failed to parse NPq::NProto::TDqReadTaskParams from TaskParams");
        result.emplace_back(std::move(params));
    }
    return result;
}

std::vector<TPartitionKey> GetPartitionsToRead(
    const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams,
    const std::vector<TPartitionKey>& federatedClusters
) {
    if (federatedClusters.empty()) {
        return GetPartitionsToRead(readTaskParams, Nothing());
    }

    std::vector<TPartitionKey> result;
    for (const auto& federatedCluster : federatedClusters) {
        const auto partitionKeys = GetPartitionsToRead(readTaskParams, federatedCluster);
        result.insert(result.end(), partitionKeys.begin(), partitionKeys.end());
    }
    return result;
}

} // namespace NYql::NDq
