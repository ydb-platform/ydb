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

} // namespace NYql::NPq
