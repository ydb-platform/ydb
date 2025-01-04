#include "initializer.h"
#include "secret.h"
#include "access.h"

namespace NKikimr::NMetadata::NSecret {

void TSecretInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TSecret::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key(TSecret::TDecoder::OwnerUserId);
        request.add_primary_key(TSecret::TDecoder::SecretId);
        {
            auto& column = *request.add_columns();
            column.set_name(TSecret::TDecoder::OwnerUserId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TSecret::TDecoder::SecretId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TSecret::TDecoder::Value);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TSecret::AddHistoryTableScheme(request);
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetNoAccessModifier(TSecret::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NInitializer::TACLModifierConstructor::GetNoAccessModifier(TSecret::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    {
        Ydb::Table::AlterTableRequest request;
        request.set_session_id("");
        request.set_path(TSecret::GetBehaviour()->GetStorageTablePath());
        {
            auto& index = *request.add_add_indexes();
            index.set_name("index_by_secret_id");
            index.add_index_columns(TSecret::TDecoder::SecretId);
            index.add_index_columns(TSecret::TDecoder::OwnerUserId);
            index.mutable_global_index();
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogAlterTable>(request, "add_index_by_secret_id"));
    }
    controller->OnPreparationFinished(result);
}

void TAccessInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TAccess::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key(TAccess::TDecoder::OwnerUserId);
        request.add_primary_key(TAccess::TDecoder::SecretId);
        request.add_primary_key(TAccess::TDecoder::AccessSID);
        {
            auto& column = *request.add_columns();
            column.set_name(TAccess::TDecoder::OwnerUserId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TAccess::TDecoder::SecretId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TAccess::TDecoder::AccessSID);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TAccess::AddHistoryTableScheme(request);
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetNoAccessModifier(TAccess::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NInitializer::TACLModifierConstructor::GetNoAccessModifier(TAccess::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    controller->OnPreparationFinished(result);
}

}
