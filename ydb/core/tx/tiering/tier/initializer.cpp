#include "initializer.h"
#include "object.h"

namespace NKikimr::NColumnShard::NTiers {

TVector<NKikimr::NMetadataInitializer::ITableModifier::TPtr> TTiersInitializer::BuildModifiers() const {
    TVector<NMetadataInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TTierConfig::GetStorageTablePath());
        request.add_primary_key("tierName");
        {
            auto& column = *request.add_columns();
            column.set_name("ownerPath");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierName");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("tierConfig");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request, "create_tiers"));
        auto hRequest = TTierConfig::AddHistoryTableScheme(request);
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(hRequest, "create_tiers_history"));
    }
    return result;
}

void TTiersInitializer::DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const {
    controller->PreparationFinished(BuildModifiers());
}

}
