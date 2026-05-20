#include "control.h"
#include "session.h"
#include "task.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusionStatus TImportTask::DoDeserializeFromProto(const NKikimrColumnShardImportProto::TImportTask& proto) {
    const auto& identifier = proto.GetIdentifier();
    if (identifier.HasSchemeShardLocalPathId()) {
        SchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(identifier.GetSchemeShardLocalPathId());
    } else {
        SchemeShardLocalPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(identifier.GetPathId());
    }
    if (!SchemeShardLocalPathId) {
        return TConclusionStatus::Fail("incorrect schemeShardLocalPathId (cannot be zero)");
    }
    if (!proto.HasTxId()) {
        return TConclusionStatus::Fail("Can't find tx id");
    }
    if (!proto.HasRestoreTask()) {
        return TConclusionStatus::Fail("Can't find restore task");
    }
    if (!proto.HasSchemaVersion()) {
        return TConclusionStatus::Fail("Can't find schema version");
    }
    TxId = proto.GetTxId();
    RestoreTask = proto.GetRestoreTask();
    SchemaVersion = proto.GetSchemaVersion();
    Columns.clear();
    for (const auto& columnProto : proto.GetColumns()) {
        const NKikimrProto::TTypeInfo* typeInfoProto = columnProto.HasTypeInfo() ? &columnProto.GetTypeInfo() : nullptr;
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(columnProto.GetTypeId(), typeInfoProto);
        Columns.emplace_back(columnProto.GetName(), typeInfoMod.TypeInfo);
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardImportProto::TImportTask TImportTask::DoSerializeToProto() const {
    NKikimrColumnShardImportProto::TImportTask result;
    result.MutableIdentifier()->SetSchemeShardLocalPathId(SchemeShardLocalPathId.GetRawValue());
    result.MutableIdentifier()->SetPathId(SchemeShardLocalPathId.GetRawValue());
    if (TxId) {
        result.SetTxId(*TxId);
    }
    *result.MutableRestoreTask() = RestoreTask;
    if (SchemaVersion) {
        result.SetSchemaVersion(*SchemaVersion);
    }
    for (const auto& column : Columns) {
        auto* columnProto = result.AddColumns();
        columnProto->SetName(column.first);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.second, "");
        columnProto->SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *columnProto->MutableTypeInfo() = *columnType.TypeInfo;
        }
    }
    return result;
}

NBackground::TSessionControlContainer TImportTask::BuildConfirmControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(),
        std::make_shared<TConfirmSessionControl>(GetClassName(), ::ToString(SchemeShardLocalPathId.GetRawValue())));
}

NBackground::TSessionControlContainer TImportTask::BuildAbortControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(),
        std::make_shared<TAbortSessionControl>(GetClassName(), ::ToString(SchemeShardLocalPathId.GetRawValue())));
}

std::shared_ptr<NBackground::ISessionLogic> TImportTask::DoBuildSession() const {
    auto result = std::make_shared<TSession>(std::make_shared<TImportTask>(SchemeShardLocalPathId, Columns, RestoreTask, SchemaVersion, TxId));
    if (!!TxId) {
        result->Confirm();
    }
    return result;
}

TString TImportTask::GetClassNameStatic() {
    return "CS::IMPORT";
}

TString TImportTask::GetClassName() const {
    return GetClassNameStatic();
}

const NColumnShard::TSchemeShardLocalPathId TImportTask::GetSchemeShardLocalPathId() const {
    return SchemeShardLocalPathId;
}

TImportTask::TImportTask(const NColumnShard::TSchemeShardLocalPathId& schemeShardLocalPathId, const TVector<TNameTypeInfo>& columns,
    const NKikimrSchemeOp::TRestoreTask& restoreTask, const std::optional<ui64> schemaVersion, const std::optional<ui64> txId)
    : SchemeShardLocalPathId(schemeShardLocalPathId)
    , Columns(columns)
    , RestoreTask(restoreTask)
    , TxId(txId)
    , SchemaVersion(schemaVersion)
{
}

TString TImportTask::DebugString() const {
    return TStringBuilder() << "{scheme_shard_local_path_id=" << SchemeShardLocalPathId.DebugString() << ";}";
}

}   // namespace NKikimr::NOlap::NImport
