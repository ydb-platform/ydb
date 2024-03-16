#include "session.h"
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TExportTask::DeserializeFromProto(const NKikimrColumnShardExportProto::TExportTask& proto) {
    auto id = TIdentifier::BuildFromProto(proto.GetIdentifier());
    if (!id) {
        return id;
    }
    auto selector = TSelectorContainer::BuildFromProto(proto.GetSelector());
    if (!selector) {
        return selector;
    }
    Identifier = id.DetachResult();
    Selector = selector.DetachResult();
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TExportTask TExportTask::SerializeToProto() const {
    NKikimrColumnShardExportProto::TExportTask result;
    *result.MutableIdentifier() = Identifier.SerializeToProto();
    *result.MutableSelector() = Selector.SerializeToProto();
    return result;
}

NKikimr::TConclusion<NKikimr::NOlap::NExport::TExportTask> TExportTask::BuildFromProto(const NKikimrColumnShardExportProto::TExportTask& proto) {
    TExportTask result;
    auto resultParsed = result.DeserializeFromProto(proto);
    if (!resultParsed) {
        return resultParsed;
    }
    return result;
}

}
