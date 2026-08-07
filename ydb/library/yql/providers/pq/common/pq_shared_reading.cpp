#include "pq_shared_reading.h"

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

namespace NYql {

bool HasSharedReading(const ::google::protobuf::Any& maybePqTopicSource) {
    NPq::NProto::TDqPqTopicSource source;
    if (!maybePqTopicSource.UnpackTo(&source)) {
        return false;
    }
    return source.GetSharedReading();
}

} // namespace NYql
