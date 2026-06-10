#include "pq_partitions.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/vector.h>

namespace NYql::NDq {

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

std::vector<ui64> GetPartitionsToRead(const std::vector<NPq::NProto::TDqReadTaskParams>& readTaskParams) {
    std::vector<ui64> result;
    for (const auto& readTaskParams : readTaskParams) {
        for (const auto& partitioningParams : readTaskParams.GetPartitioningParams()) {
            for (
                auto pk = partitioningParams.GetEachTopicPartitionGroupId();
                pk < partitioningParams.GetTopicPartitionsCount();
                pk += partitioningParams.GetDqPartitionsCount()
            ) {
                result.emplace_back(pk); // 0-based in topic API
            }
        }
    }
    return result;
}

} // namespace NYql::NDq
