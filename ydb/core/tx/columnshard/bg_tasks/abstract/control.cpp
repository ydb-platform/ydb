#include "control.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

NKikimrTxBackgroundProto::TSessionControlContainer TSessionControlContainer::SerializeToProto() const {
    NKikimrTxBackgroundProto::TSessionControlContainer result;
    *result.MutableStatusChannelContainer() = ChannelContainer.SerializeToString();
    *result.MutableLogicControlContainer() = LogicControlContainer.SerializeToProto();
    return result;
}

NKikimr::TConclusionStatus TSessionControlContainer::DeserializeFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& proto) {
    if (!ChannelContainer.DeserializeFromString(proto.GetStatusChannelContainer())) {
        return TConclusionStatus::Fail("cannot parse channel from proto");
    }
    if (!LogicControlContainer.DeserializeFromProto(proto.GetLogicControlContainer())) {
        return TConclusionStatus::Fail("cannot parse logic from proto");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus ISessionLogicControl::DeserializeFromProto(const TProto& data) {
    SessionClassName = data.GetSessionClassName();
    SessionIdentifier = data.GetSessionIdentifier();
    return DeserializeFromString(data.GetSessionControlDescription());
}

void ISessionLogicControl::SerializeToProto(TProto& proto) const {
    proto.SetSessionClassName(SessionClassName);
    proto.SetSessionIdentifier(SessionIdentifier);
    proto.SetSessionControlDescription(SerializeToString());
}

}