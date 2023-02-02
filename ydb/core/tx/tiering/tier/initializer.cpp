#include "initializer.h"
#include "object.h"

namespace NKikimr::NColumnShard::NTiers {

TVector<NKikimr::NMetadata::NInitializer::ITableModifier::TPtr> TTiersInitializer::BuildModifiers() const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TTierConfig::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key("tierName");
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierConfig");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TTierConfig::AddHistoryTableScheme(request);
        result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTierConfig::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTierConfig::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    return result;
}

void TTiersInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    controller->OnPreparationFinished(BuildModifiers());
}

}
