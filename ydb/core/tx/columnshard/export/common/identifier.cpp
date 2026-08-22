#include "identifier.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TIdentifier::DeserializeFromProto(const NKikimrColumnShardExportProto::TIdentifier& proto) {
    if (proto.HasSchemeShardLocalPathId()) {
        SchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(proto.GetSchemeShardLocalPathId());
    } else {
        SchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(proto.GetPathId());
    }
    if (!SchemeShardLocalPathId) {
        return TConclusionStatus::Fail("Incorrect schemeShardLocalPathId (zero)");
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
    result.SchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(proto.GetBackupTask().GetTableId());
    if (!result.SchemeShardLocalPathId) {
        return TConclusionStatus::Fail("incorrect schemeShardLocalPathId (cannot be zero)");
    }
    return result;
}

NKikimrColumnShardExportProto::TIdentifier TIdentifier::SerializeToProto() const {
    NKikimrColumnShardExportProto::TIdentifier result;
    result.SetPathId(SchemeShardLocalPathId.GetRawValue());
    result.SetSchemeShardLocalPathId(SchemeShardLocalPathId.GetRawValue());
    return result;
}

TString TIdentifier::DebugString() const {
    return SerializeToProto().DebugString();
}

TString TIdentifier::ToString() const {
    return TStringBuilder() << "scheme_shard_local_path_id=" << SchemeShardLocalPathId << ";";
}

}   // namespace NKikimr::NOlap::NExport
