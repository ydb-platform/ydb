#include "task.h"
#include "session.h"
#include "control.h"
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusionStatus TImportTask::DoDeserializeFromProto(const NKikimrColumnShardImportProto::TImportTask& proto) {
    InternalPathId = TInternalPathId::FromRawValue(proto.GetIdentifier().GetPathId());
    if (proto.HasTxId()) {
        TxId = proto.GetTxId();
    }
    if (proto.HasRestoreTask()) {
        RestoreTask = proto.GetRestoreTask();
    }
    if (proto.HasSchemaVersion()) {
        SchemaVersion = proto.GetSchemaVersion();
    }
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
    result.MutableIdentifier()->SetPathId(InternalPathId.GetRawValue());
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
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TConfirmSessionControl>(GetClassName(), ::ToString(InternalPathId.DebugString())));
}

NBackground::TSessionControlContainer TImportTask::BuildAbortControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TAbortSessionControl>(GetClassName(), ::ToString(InternalPathId.DebugString())));
}

std::shared_ptr<NBackground::ISessionLogic> TImportTask::DoBuildSession() const {
    auto result = std::make_shared<TSession>(std::make_shared<TImportTask>(InternalPathId, Columns, RestoreTask, SchemaVersion, TxId));
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

const TInternalPathId TImportTask::GetInternalPathId() const {
    return InternalPathId;
}

TImportTask::TImportTask(const TInternalPathId &internalPathId,
                         const TVector<TNameTypeInfo>& columns,
                         const NKikimrSchemeOp::TRestoreTask& restoreTask,
                         const std::optional<ui64> schemaVersion,
                         const std::optional<ui64> txId)
    : InternalPathId(internalPathId)
    , Columns(columns)
    , RestoreTask(restoreTask)
    , TxId(txId)
    , SchemaVersion(schemaVersion) {
}

TString TImportTask::DebugString() const {
    return TStringBuilder() << "{internal_path_id=" << InternalPathId.DebugString() << ";}";
}

} // namespace NKikimr::NOlap::NImport
