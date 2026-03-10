#include "service_operation.h"

#include "operation_helpers.h"
#include "rpc_backup_base.h"
#include "rpc_restore_base.h"
#include "rpc_export_base.h"
#include "rpc_import_base.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_forced_compaction.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/events/global.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeShard;
using namespace NKikimrIssues;
using namespace NOperationId;
using namespace Ydb;

using TEvListOperationsRequest = TGrpcRequestNoOperationCall<Ydb::Operations::ListOperationsRequest,
    Ydb::Operations::ListOperationsResponse>;

class TListOperationsRPC
    : public TRpcOperationRequestActor<TListOperationsRPC, TEvListOperationsRequest>
    , public TExportConv
    , public TBackupCollectionRestoreConv
{
    ui32 GetRequiredAccessRights() const override {
        return NACLib::GenericRead;
    }

    TStringBuf GetLogPrefix() const override {
        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::EXPORT:
            return "[ListExports]";
        case TOperationId::IMPORT:
            return "[ListImports]";
        case TOperationId::BUILD_INDEX:
            return "[ListIndexBuilds]";
        case TOperationId::SCRIPT_EXECUTION:
            return "[ListScriptExecutions]";
        case TOperationId::SS_BG_TASKS:
            return "[SchemeShardTasks]";
        case TOperationId::INCREMENTAL_BACKUP:
            return "[ListIncrementalBackups]";
        case TOperationId::RESTORE:
            return "[ListBackupCollectionRestores]";
        case TOperationId::COMPACTION:
            return "[ListForcedCompactions]";
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        const auto& request = *GetProtoRequest();

        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::SS_BG_TASKS:
            return new NSchemeShard::NBackground::TEvListRequest(GetDatabaseName(), request.page_size(), request.page_token());
        case TOperationId::EXPORT:
            return new TEvExport::TEvListExportsRequest(GetDatabaseName(), request.page_size(), request.page_token(), request.kind());
        case TOperationId::IMPORT:
            return new TEvImport::TEvListImportsRequest(GetDatabaseName(), request.page_size(), request.page_token(), request.kind());
        case TOperationId::BUILD_INDEX:
            return new TEvIndexBuilder::TEvListRequest(GetDatabaseName(), request.page_size(), request.page_token());
        case TOperationId::INCREMENTAL_BACKUP:
            return new TEvBackup::TEvListIncrementalBackupsRequest(GetDatabaseName(), request.page_size(), request.page_token());
        case TOperationId::RESTORE:
            return new TEvBackup::TEvListBackupCollectionRestoresRequest(GetDatabaseName(), request.page_size(), request.page_token());
        case TOperationId::COMPACTION:
            return new TEvForcedCompaction::TEvListRequest(GetDatabaseName(), request.page_size(), request.page_token());
        default:
            Y_ABORT("unreachable");
        }
    }

    void Handle(TEvExport::TEvListExportsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvListExportsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TExportConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvImport::TEvListImportsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvListImportsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TImportConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvIndexBuilder::TEvListResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvListResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;

        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            auto operation = response.add_operations();
            ::NKikimr::NGRpcService::ToOperation(entry, operation);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvForcedCompaction::TEvListResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvForcedCompaction::TEvListResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;

        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            auto operation = response.add_operations();
            ::NKikimr::NGRpcService::ToOperation(entry, operation);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(NSchemeShard::NBackground::TEvListResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvSchemeShard::TEvBGTasksListResponse: record# " << record.ShortDebugString());

        TResponse response;

        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = entry;
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void SendListScriptExecutions() {
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvListScriptExecutionOperations(GetDatabaseName(), GetProtoRequest()->page_size(), GetProtoRequest()->page_token(), GetUserSID(*Request)));
    }

    void Handle(NKqp::TEvListScriptExecutionOperationsResponse::TPtr& ev) {
        TResponse response;
        response.set_status(ev->Get()->Status);
        for (const NYql::TIssue& issue : ev->Get()->Issues) {
            NYql::IssueToMessage(issue, response.add_issues());
        }
        for (auto& op : ev->Get()->Operations) {
            response.add_operations()->Swap(&op);
        }
        response.set_next_page_token(ev->Get()->NextPageToken);
        Reply(response);
    }

    void Handle(TEvBackup::TEvListIncrementalBackupsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvListIncrementalBackupsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TIncrementalBackupConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

    void Handle(TEvBackup::TEvListBackupCollectionRestoresResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvListBackupCollectionRestoresResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TBackupCollectionRestoreConv::ToOperation(entry);
        }
        response.set_next_page_token(record.GetNextPageToken());
        Reply(response);
    }

public:
    using TRpcOperationRequestActor::TRpcOperationRequestActor;

    void Bootstrap() {
        Become(&TListOperationsRPC::StateWait);

        switch (ParseKind(GetProtoRequest()->kind())) {
        case TOperationId::EXPORT:
        case TOperationId::IMPORT:
        case TOperationId::BUILD_INDEX:
        case TOperationId::SS_BG_TASKS:
        case TOperationId::INCREMENTAL_BACKUP:
        case TOperationId::RESTORE:
        case TOperationId::COMPACTION:
            break;
        case TOperationId::SCRIPT_EXECUTION:
            SendListScriptExecutions();
            return;

        default:
            return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Unknown operation kind");
        }

        ResolveDatabase();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvListExportsResponse, Handle);
            hFunc(TEvImport::TEvListImportsResponse, Handle);
            hFunc(NSchemeShard::NBackground::TEvListResponse, Handle);
            hFunc(TEvIndexBuilder::TEvListResponse, Handle);
            hFunc(TEvForcedCompaction::TEvListResponse, Handle);
            hFunc(NKqp::TEvListScriptExecutionOperationsResponse, Handle);
            hFunc(TEvBackup::TEvListIncrementalBackupsResponse, Handle);
            hFunc(TEvBackup::TEvListBackupCollectionRestoresResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

}; // TListOperationsRPC

void DoListOperationsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TListOperationsRPC(p.release()));
}

template<>
IActor* TEvListOperationsRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TListOperationsRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
