#include "initializer.h"
#include "object.h"

namespace NKikimr::NMetadata::NCSIndex {

void TInitializer::DoPrepare(NInitializer::IInitializerInput::TPtr controller) const {
    TVector<NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(TObject::GetBehaviour()->GetStorageTablePath());
        request.add_primary_key(TObject::TDecoder::IndexId);
        request.add_primary_key(TObject::TDecoder::TablePath);
        {
            auto& column = *request.add_columns();
            column.set_name(TObject::TDecoder::IndexId);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TObject::TDecoder::TablePath);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TObject::TDecoder::Active);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::BOOL);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TObject::TDecoder::Delete);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::BOOL);
        }
        {
            auto& column = *request.add_columns();
            column.set_name(TObject::TDecoder::Extractor);
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(request, "create"));
        auto hRequest = TObject::AddHistoryTableScheme(request);
        result.emplace_back(new NInitializer::TGenericTableModifier<NRequest::TDialogCreateTable>(hRequest, "create_history"));
    }
    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TObject::GetBehaviour()->GetStorageTablePath(), "acl"));
    result.emplace_back(NInitializer::TACLModifierConstructor::GetReadOnlyModifier(TObject::GetBehaviour()->GetStorageHistoryTablePath(), "acl_history"));
    controller->OnPreparationFinished(result);
}

}
