#include "activation.h"
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NCSIndex {

NKikimr::NMetadata::NRequest::TDialogYQLRequest::TRequest TActivation::BuildUpdateRequest() const {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $indexId AS Utf8;" << Endl;
    sb << "DECLARE $tablePath AS Utf8;" << Endl;
    sb << "UPDATE `" + NMetadata::NCSIndex::TObject::GetBehaviour()->GetStorageTablePath() + "`" << Endl;
    sb << "SET active = true" << Endl;
    sb << "WHERE indexId = $indexId" << Endl;
    sb << "AND tablePath = $tablePath" << Endl;
    request.mutable_query()->set_yql_text(sb);

    {
        auto& param = (*request.mutable_parameters())["$indexId"];
        param.mutable_value()->set_text_value(Object.GetIndexId());
        param.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& param = (*request.mutable_parameters())["$tablePath"];
        param.mutable_value()->set_text_value(Object.GetTablePath());
        param.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TActivation::OnDescriptionSuccess(NMetadata::NProvider::TTableInfo&& result, const TString& /*requestId*/) {
    if (!result->ColumnTableInfo) {
        ExternalController->OnActivationFailed(Ydb::StatusIds::INTERNAL_ERROR, "incorrect column table info", RequestId);
        SelfContainer = nullptr;
        return;
    }
    Ydb::Table::CreateTableRequest request;
    if (!Object.TryProvideTtl(result->ColumnTableInfo->Description, &request)) {
        ExternalController->OnActivationFailed(Ydb::StatusIds::INTERNAL_ERROR, "cannot convert ttl method from column tables", RequestId);
        SelfContainer = nullptr;
        return;
    }

    request.set_session_id("");
    request.set_path(Object.GetIndexTablePath());
    request.add_primary_key("index_hash");
    auto pkFields = result.GetPKFields();
    for (auto&& i : pkFields) {
        request.add_primary_key("pk_" + i.Name);
    }
    {
        auto& column = *request.add_columns();
        column.set_name("index_hash");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    }
    for (auto&& i : pkFields) {
        auto& column = *request.add_columns();
        column.set_name("pk_" + i.Name);
        auto primitiveType = NMetadata::NInternal::TYDBType::ConvertYQLToYDB(i.PType.GetTypeId());
        if (!primitiveType) {
            ExternalController->OnActivationFailed(Ydb::StatusIds::INTERNAL_ERROR, "cannot convert type yql -> ydb", RequestId);
            SelfContainer = nullptr;
            return;
        }
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(*primitiveType);
    }
    NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable> modifier(request, "create");
    modifier.Execute(SelfContainer, Config.GetRequestConfig());
}

void TActivation::OnModificationFinished(const TString& modificationId) {
    if (modificationId == "create") {
        NMetadata::NInitializer::ITableModifier::TPtr modifier =
            NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(Object.GetIndexTablePath(), "access");
        modifier->Execute(SelfContainer, Config.GetRequestConfig());
    } else if (modificationId == "access") {
        NMetadata::NRequest::TYQLRequestExecutor::Execute(BuildUpdateRequest(), NACLib::TSystemUsers::Metadata(), SelfContainer);
    } else {
        Y_ABORT_UNLESS(false);
    }
}

void TActivation::OnModificationFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage, const TString& /*modificationId*/) {
    ExternalController->OnActivationFailed(status, errorMessage, RequestId);
    SelfContainer = nullptr;
}

void TActivation::Start(std::shared_ptr<TActivation> selfContainer) {
    Y_ABORT_UNLESS(!!selfContainer);
    SelfContainer = selfContainer;
    TActivationContext::ActorSystem()->Register(new NMetadata::NProvider::TSchemeDescriptionActor(SelfContainer, RequestId, Object.GetTablePath()));
}

}
