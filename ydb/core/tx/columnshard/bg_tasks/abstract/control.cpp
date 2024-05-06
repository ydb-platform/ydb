#include "control.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

NKikimrTxBackgroundProto::TSessionControlContainer TSessionControlContainer::SerializeToProto() const {
    NKikimrTxBackgroundProto::TSessionControlContainer result;
    result.SetSessionClassName(SessionClassName);
    result.SetSessionIdentifier(SessionIdentifier);
    result.SetStatusChannelContainer(ChannelContainer.SerializeToString());
    result.SetLogicControlContainer(LogicControlContainer.SerializeToString());
    return result;
}

NKikimr::TConclusionStatus TSessionControlContainer::DeserializeFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& proto) {
    SessionClassName = proto.GetSessionClassName();
    SessionIdentifier = proto.GetSessionIdentifier();
    if (!SessionClassName) {
        return TConclusionStatus::Fail("incorrect session class name for bg_task");
    }
    if (!SessionIdentifier) {
        return TConclusionStatus::Fail("incorrect session id for bg_task");
    }
    if (!ChannelContainer.DeserializeFromString(proto.GetStatusChannelContainer())) {
        return TConclusionStatus::Fail("cannot parse channel from proto");
    }
    if (!LogicControlContainer.DeserializeFromString(proto.GetLogicControlContainer())) {
        return TConclusionStatus::Fail("cannot parse logic from proto");
    }
    return TConclusionStatus::Success();
}

}