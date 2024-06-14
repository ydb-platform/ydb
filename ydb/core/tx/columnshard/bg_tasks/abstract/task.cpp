#include "task.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

NKikimrTxBackgroundProto::TTaskContainer TTask::SerializeToProto() const {
    NKikimrTxBackgroundProto::TTaskContainer result;
    result.SetIdentifier(Identifier);
    result.SetStatusChannelContainer(ChannelContainer.SerializeToString());
    result.SetTaskDescriptionContainer(DescriptionContainer.SerializeToString());
    return result;
}

NKikimr::TConclusionStatus TTask::DeserializeFromProto(const NKikimrTxBackgroundProto::TTaskContainer& proto) {
    Identifier = proto.GetIdentifier();
    if (!Identifier) {
        return TConclusionStatus::Fail("empty identifier is not correct for bg_task");
    }
    if (!ChannelContainer.DeserializeFromString(proto.GetStatusChannelContainer())) {
        return TConclusionStatus::Fail("cannot parse status channel from proto");
    }
    if (!DescriptionContainer.DeserializeFromString(proto.GetTaskDescriptionContainer())) {
        return TConclusionStatus::Fail("cannot parse task description from proto");
    }
    return TConclusionStatus::Success();
}

}