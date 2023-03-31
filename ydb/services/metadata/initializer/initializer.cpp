#include "initializer.h"

namespace NKikimr::NMetadata::NInitializer {

void TInitializer::DoPrepare(IInitializerInput::TPtr controller) const {
    TVector<ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TDBInitialization::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key(TDBInitialization::TDecoder::ComponentId);
        request.add_primary_key(TDBInitialization::TDecoder::ModificationId);
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::ComponentId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::ModificationId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::Instant);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        result.emplace_back(new TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
    }
    result.emplace_back(TACLModifierConstructor::GetReadOnlyModifier(TDBInitialization::GetBehaviour()->GetStorageTablePath(), "acl"));
    controller->OnPreparationFinished(result);
}

}
