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

void TStreamingQueryInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_path(TStreamingQueryConfig::GetBehaviour()->GetStorageTablePath());
        AddColumn(request, TStreamingQueryConfig::DatabaseIdColumn, Ydb::Type::UTF8, true);
        AddColumn(request, TStreamingQueryConfig::QueryPathColumn, Ydb::Type::UTF8, true);
        AddColumn(request, TStreamingQueryConfig::StateColumn, Ydb::Type::JSON_DOCUMENT);
        result.emplace_back(std::make_shared<NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>>(request, "create"));
    }
    result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetNoAccessModifier(TStreamingQueryConfig::GetBehaviour()->GetStorageTablePath(), "acl"));
    controller->OnPreparationFinished(result);
}

}  // namespace NKikimr::NKqp
