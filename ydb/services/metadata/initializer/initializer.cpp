#include "initializer.h"

namespace NKikimr::NMetadataInitializer {

void TInitializer::DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const {
    TVector<NMetadataInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TDBInitialization::GetStorageTablePath());
        request.add_primary_key(TDBInitialization::TDecoder::ComponentId);
        request.add_primary_key(TDBInitialization::TDecoder::ModificationId);
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::ComponentId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::ModificationId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TDBInitialization::TDecoder::Instant);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request, "create"));
    }
    {
        Ydb::Scheme::ModifyPermissionsRequest request;
        request.set_path(TDBInitialization::GetStorageTablePath());
        request.set_clear_permissions(true);
        auto* permission = request.add_actions();
        permission->mutable_grant()->set_subject("*");
        permission->mutable_grant()->add_permission_names("ydb.tables.read");
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogModifyPermissions>(request, "acl"));
    }
    controller->PreparationFinished(result);
}

}
