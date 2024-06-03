#include "snapshot.h"
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TSnapshot::DebugString() const {
    return TStringBuilder() << "plan_step=" << PlanStep << ";tx_id=" << TxId << ";";
}

NKikimrColumnShardProto::TSnapshot TSnapshot::SerializeToProto() const {
    NKikimrColumnShardProto::TSnapshot result;
    SerializeToProto(result);
    return result;
}

TConclusionStatus TSnapshot::DeserializeFromString(const TString& data) {
    NKikimrColumnShardProto::TSnapshot proto;
    if (!proto.ParseFromArray(data.data(), data.size())) {
        return TConclusionStatus::Fail("cannot parse string as snapshot proto");
    }
    return DeserializeFromProto(proto);
}

TString TSnapshot::SerializeToString() const {
    return SerializeToProto().SerializeAsString();
}

};
