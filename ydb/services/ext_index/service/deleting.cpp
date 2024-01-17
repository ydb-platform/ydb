#include "deleting.h"
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NCSIndex {

NKikimr::NMetadata::NRequest::TDialogYQLRequest::TRequest TDeleting::BuildDeleteRequest() const {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $indexId AS Utf8;" << Endl;
    sb << "DECLARE $tablePath AS Utf8;" << Endl;
    sb << "DELETE FROM `" + NMetadata::NCSIndex::TObject::GetBehaviour()->GetStorageTablePath() + "`" << Endl;
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

void TDeleting::Start(std::shared_ptr<TDeleting> selfContainer) {
    Y_ABORT_UNLESS(!!selfContainer);
    SelfContainer = selfContainer;

    Ydb::Table::DropTableRequest request;
    request.set_session_id("");
    request.set_path(Object.GetIndexTablePath());
    NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogDropTable> dropTable(request, "drop");
    dropTable.Execute(SelfContainer, Config.GetRequestConfig());
}

void TDeleting::OnModificationFinished(const TString& /*modificationId*/) {
    NMetadata::NRequest::TYQLRequestExecutor::Execute(BuildDeleteRequest(), NACLib::TSystemUsers::Metadata(), SelfContainer);
}

void TDeleting::OnModificationFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage, const TString& /*modificationId*/) {
    ExternalController->OnDeletingFailed(status, errorMessage, RequestId);
    SelfContainer = nullptr;
}

}
