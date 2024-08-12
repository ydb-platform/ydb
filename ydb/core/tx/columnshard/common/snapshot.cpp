#include "snapshot.h"
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TSnapshot::DebugString() const {
    return TStringBuilder() << "plan_step=" << PlanStep << ";tx_id=" << TxId << ";";
}

NJson::TJsonValue TSnapshot::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("plan_step", PlanStep);
    result.InsertValue("tx_id", TxId);
    return result;
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

NKikimr::NOlap::TSnapshot TSnapshot::MaxForPlanStep(const ui64 planStep) noexcept {
    return TSnapshot(planStep, ::Max<ui64>());
}

NKikimr::NOlap::TSnapshot TSnapshot::MaxForPlanInstant(const TInstant planInstant) noexcept {
    return TSnapshot(planInstant.MilliSeconds(), ::Max<ui64>());
}

};
