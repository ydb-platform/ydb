#include "initializer.h"
#include "object.h"


namespace NKikimr::NKqp {

namespace {

void AddColumn(Ydb::Table::CreateTableRequest& request, const TString& name, Ydb::Type::PrimitiveTypeId type, bool primary = false) {
    if (primary) {
        request.add_primary_key(name);
    }

    auto& column = *request.add_columns();
    column.set_name(name);
    column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(type);
}

}  // anonymous namespace

void TResourcePoolClassifierInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath());
        AddColumn(request, TResourcePoolClassifierConfig::TDecoder::Database, Ydb::Type::UTF8, true);
        AddColumn(request, TResourcePoolClassifierConfig::TDecoder::Name, Ydb::Type::UTF8, true);
        AddColumn(request, TResourcePoolClassifierConfig::TDecoder::Rank, Ydb::Type::INT64);
        AddColumn(request, TResourcePoolClassifierConfig::TDecoder::ResourcePool, Ydb::Type::UTF8);
        AddColumn(request, TResourcePoolClassifierConfig::TDecoder::Membername, Ydb::Type::UTF8);
        result.emplace_back(std::make_shared<NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>>(request, "create"));

        auto historyRequest = TResourcePoolClassifierConfig::AddHistoryTableScheme(request);
        result.emplace_back(std::make_shared<NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>>(historyRequest, "create_history"));
    }
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TResourcePoolClassifierConfig::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    controller->OnPreparationFinished(result);
}

}  // namespace NKikimr::NKqp
