#include "task_meta.h"

#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

namespace NYql::NPq {

TMaybe<TTopicPartitionsSet> GetTopicPartitionsSet(const google::protobuf::Any& dqTaskMeta) {
    if (dqTaskMeta.Is<Yql::DqsProto::TTaskMeta>()) {
        Yql::DqsProto::TTaskMeta meta;
        if (dqTaskMeta.UnpackTo(&meta)) {
            auto pqReaderParams = meta.GetTaskParams().find("pq");
            if (pqReaderParams != meta.GetTaskParams().end()) {
                NYql::NPq::NProto::TDqReadTaskParams readTaskParams;
                if (readTaskParams.ParseFromString(pqReaderParams->second)) {
                    return TTopicPartitionsSet{
                        readTaskParams.GetPartitioningParams().GetEachTopicPartitionGroupId(),
                        readTaskParams.GetPartitioningParams().GetDqPartitionsCount(),
                        readTaskParams.GetPartitioningParams().GetTopicPartitionsCount()};
                }
            }
        }
    }
    return Nothing();
}

std::vector<TTopicPartitionsSet> GetTopicPartitionsSets(const NDqProto::TDqTask& dqTask) {
    if (auto partitionSet = GetTopicPartitionsSet(dqTask.GetMeta())) {
        return {std::move(*partitionSet)};
    }

    std::vector<TTopicPartitionsSet> result;
    result.reserve(dqTask.ReadRangesSize());
    for (const auto& readRange : dqTask.GetReadRanges()) {
        NPq::NProto::TDqReadTaskParams readTaskParams;
        if (!readTaskParams.ParseFromString(readRange)) {
            return {};
        }

        const auto& params = readTaskParams.GetPartitioningParams();
        result.push_back({
            .EachTopicPartitionGroupId = params.GetEachTopicPartitionGroupId(),
            .DqPartitionsCount = params.GetDqPartitionsCount(),
            .TopicPartitionsCount = params.GetTopicPartitionsCount()
        });
    }

    return result;
}

} // namespace NYql::NPq
