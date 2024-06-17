#include "operator.h"

namespace NKikimr::NOlap::NStatistics {

bool IOperator::DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) {
    if (!TryFromString(proto.GetClassName(), Type)) {
        return false;
    }
    return DoDeserializeFromProto(proto);
}

}