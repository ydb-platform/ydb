#include "initializer.h"
#include "object.h"

namespace NKikimr::NColumnShard::NTiers {

TVector<NKikimr::NMetadataInitializer::ITableModifier::TPtr> TTierRulesInitializer::BuildModifiers() const {
    TVector<NMetadataInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TTieringRule::GetStorageTablePath());
        request.add_primary_key("tieringRuleId");
        {
            auto& column = *request.add_columns();
            column.set_name("tieringRuleId");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("defaultColumn");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("description");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TTieringRule::AddHistoryTableScheme(request);
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NMetadataInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTieringRule::GetStorageTablePath(), "acl"));
    result.emplace_back(NMetadataInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTieringRule::GetStorageHistoryTablePath(), "acl_history"));
    return result;
}

void TTierRulesInitializer::DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const {
    controller->PreparationFinished(BuildModifiers());
}

}
