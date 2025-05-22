#include "session.h"
#include "control.h"

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusionStatus TExportTask::DoDeserializeFromProto(const NKikimrColumnShardExportProto::TExportTask& proto) {
    auto id = TIdentifier::BuildFromProto(proto.GetIdentifier());
    if (!id) {
        return id;
    }
    auto selector = TSelectorContainer::BuildFromProto(proto.GetSelector());
    if (!selector) {
        return selector;
    }
    auto initializer = TStorageInitializerContainer::BuildFromProto(proto.GetStorageInitializer());
    if (!initializer) {
        return initializer;
    }
    auto serializer = NArrow::NSerialization::TSerializerContainer::BuildFromProto(proto.GetSerializer());
    if (!serializer) {
        return serializer;
    }
    Identifier = id.DetachResult();
    Selector = selector.DetachResult();
    StorageInitializer = initializer.DetachResult();
    Serializer = serializer.DetachResult();
    if (proto.HasTxId()) {
        TxId = proto.GetTxId();
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardExportProto::TExportTask TExportTask::DoSerializeToProto() const {
    NKikimrColumnShardExportProto::TExportTask result;
    *result.MutableIdentifier() = Identifier.SerializeToProto();
    *result.MutableSelector() = Selector.SerializeToProto();
    *result.MutableStorageInitializer() = StorageInitializer.SerializeToProto();
    *result.MutableSerializer() = Serializer.SerializeToProto();
    if (TxId) {
        result.SetTxId(*TxId);
    }
    return result;
}

NBackground::TSessionControlContainer TExportTask::BuildConfirmControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TConfirmSessionControl>(GetClassName(), ::ToString(Identifier.GetPathId())));
}

NBackground::TSessionControlContainer TExportTask::BuildAbortControl() const {
    return NBackground::TSessionControlContainer(std::make_shared<NBackground::TFakeStatusChannel>(), std::make_shared<TAbortSessionControl>(GetClassName(), ::ToString(Identifier.GetPathId())));
}

std::shared_ptr<NBackground::ISessionLogic> TExportTask::DoBuildSession() const {
    auto result = std::make_shared<TSession>(std::make_shared<TExportTask>(Identifier, Selector, StorageInitializer, Serializer, TxId));
    if (!!TxId) {
        result->Confirm();
    }
    return result;
}

}
