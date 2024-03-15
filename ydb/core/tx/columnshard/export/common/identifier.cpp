#include "identifier.h"
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TIdentifier::DeserializeFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto) {
    PathId = proto.GetPathId();
    if (!PathId) {
        return TConclusionStatus::Fail("Incorrect pathId (zero)");
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<NKikimr::NOlap::NExport::TIdentifier> TIdentifier::BuildFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto) {
    TIdentifier result;
    auto parseResult = result.DeserializeFromProto(proto);
    if (!parseResult) {
        return parseResult;
    }
    return result;
}

NKikimr::TConclusion<NKikimr::NOlap::NExport::TIdentifier> TIdentifier::BuildFromProto(const NKikimrTxColumnShard::TBackupTxBody& proto) {
    TIdentifier result;
    result.PathId = proto.GetBackupTask().GetTableId();
    if (!result.PathId) {
        return TConclusionStatus::Fail("incorrect pathId (cannot been zero)");
    }
    return result;
}

NKikimrColumnShardExportProto::TIdentifier TIdentifier::SerializeToProto() const {
    NKikimrColumnShardExportProto::TIdentifier result;
    result.SetPathId(PathId);
    return result;
}

TString TIdentifier::DebugString() const {
    return SerializeToProto().DebugString();
}

TString TIdentifier::ToString() const {
    return TStringBuilder() << "path_id=" << PathId << ";";
}

}