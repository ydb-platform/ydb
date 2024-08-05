#include "initializer.h"
#include "object.h"


namespace NKikimr::NKqp {

void TResourcePoolClassifierInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key(TResourcePoolClassifierConfig::TDecoder::Name);
        {
            auto& column = *request.add_columns();
            column.set_name(TResourcePoolClassifierConfig::TDecoder::Name);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TResourcePoolClassifierConfig::TDecoder::Rank);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TResourcePoolClassifierConfig::TDecoder::Config);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::JSON_DOCUMENT);
        }
        result.emplace_back(std::make_shared<NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>>(request, "create"));

        auto historyRequest = TResourcePoolClassifierConfig::AddHistoryTableScheme(request);
        result.emplace_back(std::make_shared<NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>>(historyRequest, "create_history"));
    }
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
}

}  // namespace NKikimr::NKqp
