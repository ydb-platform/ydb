#include "session.h"
#include "control.h"
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TExportTask::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TExportTask& proto) {
    auto id = TIdentifier::BuildFromProto(proto.GetIdentifier());
    if (!id) {
        return id;
    }
    Identifier = id.DetachResult();
    if (!proto.HasTxId()) {
        return TConclusionStatus::Fail("Can't find tx id");
    }
    if (!proto.HasBackupTask()) {
        return TConclusionStatus::Fail("Can't find backup task");
    }
    TxId = proto.GetTxId();
    BackupTask = proto.GetBackupTask();
    Columns.clear();
    for (const auto& columnProto : proto.GetColumns()) {
        const NKikimrProto::TTypeInfo* typeInfoProto = columnProto.HasTypeInfo() ? &columnProto.GetTypeInfo() : nullptr;
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(columnProto.GetTypeId(), typeInfoProto);
        Columns.emplace_back(columnProto.GetName(), typeInfoMod.TypeInfo);
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TExportTask TExportTask::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TExportTask result;
    *result.MutableIdentifier() = Identifier.SerializeToProto();
    if (TxId) {
        result.SetTxId(*TxId);
    }
    *result.MutableBackupTask() = BackupTask;
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

NBackground::TSessionControlContainer TExportTask::BuildConfirmControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TConfirmSessionControl>(GetClassName(), ::ToString(Identifier.GetPathId())));
}

NBackground::TSessionControlContainer TExportTask::BuildAbortControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TAbortSessionControl>(GetClassName(), ::ToString(Identifier.GetPathId())));
}

TExportTask::TExportTask(const TIdentifier& id, const std::vector<TNameTypeInfo>& columns, const NKikimrSchemeOp::TBackupTask& backupTask, const std::optional<ui64> txId)
    : Identifier(id)
    , BackupTask(backupTask)
    , TxId(txId)
    , Columns(columns)
{
}

TString TExportTask::GetClassNameStatic() {
    return "CS::EXPORT";
}

TString TExportTask::GetClassName() const {
    return GetClassNameStatic();
}

TString TExportTask::DebugString() const {
    return TStringBuilder() << "{task_id=" << Identifier.DebugString() << ";}";
}

std::shared_ptr<NBackground::ISessionLogic> TExportTask::DoBuildSession() const {
    auto result = std::make_shared<TSession>(std::make_shared<TExportTask>(Identifier, Columns, BackupTask, TxId));
    if (!!TxId) {
        result->Confirm();
    }
    return result;
}

}
