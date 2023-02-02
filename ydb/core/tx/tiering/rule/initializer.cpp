#include "initializer.h"
#include "object.h"

namespace NKikimr::NColumnShard::NTiers {

TVector<NKikimr::NMetadata::NInitializer::ITableModifier::TPtr> TTierRulesInitializer::BuildModifiers() const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TTieringRule::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key("tieringRuleId");
        {
            auto& column = *request.add_columns();
            column.set_name("tieringRuleId");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("defaultColumn");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("description");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TTieringRule::AddHistoryTableScheme(request);
        result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTieringRule::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TTieringRule::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    return result;
}

void TTierRulesInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    controller->OnPreparationFinished(BuildModifiers());
}

}
