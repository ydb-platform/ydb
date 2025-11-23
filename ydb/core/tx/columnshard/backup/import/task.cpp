#include "task.h"
#include "session.h"
#include "control.h"
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusionStatus TImportTask::DoDeserializeFromProto(const NKikimrColumnShardImportProto::TImportTask& proto) {
    InternalPathId = TInternalPathId::FromRawValue(proto.GetIdentifier().GetPathId());
    if (proto.HasTxId()) {
        TxId = proto.GetTxId();
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardImportProto::TImportTask TImportTask::DoSerializeToProto() const {
    NKikimrColumnShardImportProto::TImportTask result;
    result.MutableIdentifier()->SetPathId(InternalPathId.GetRawValue());
    if (TxId) {
        result.SetTxId(*TxId);
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
    auto result = std::make_shared<TSession>(std::make_shared<TImportTask>(InternalPathId, TxId));
    if (!!TxId) {
        result->Confirm();
    }
    return result;
}

TString TImportTask::GetClassNameStatic() { 
    return "CS::EXPORT"; 
}

TString TImportTask::GetClassName() const { 
    return GetClassNameStatic(); 
}

const TInternalPathId TImportTask::GetInternalPathId() const {
    return InternalPathId;
}

TImportTask::TImportTask(const TInternalPathId &internalPathId,
                         const std::optional<ui64> txId)
    : InternalPathId(internalPathId), TxId(txId) {
}

TString TImportTask::DebugString() const {
    return TStringBuilder() << "{internal_path_id=" << InternalPathId.DebugString() << ";}";
}

} // namespace NKikimr::NOlap::NImport
