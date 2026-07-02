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
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_forced_compaction.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/events/global.h>
#include <ydb/core/statistics/events.h>
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
        case TOperationId::FULL_BACKUP:
            return "[ListFullBackups]";
        case TOperationId::ANALYZE:
            return "[ListAnalyze]";
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
        case TOperationId::FULL_BACKUP:
            return new TEvBackup::TEvListFullBackupsRequest(GetDatabaseName(), request.page_size(), request.page_token());
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

    void ResolveStatisticsAggregatorForList() {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = GetDatabaseName();
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = TNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(GetDatabaseName());
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()),
            0, AnalyzeSaCookie);
    }

    void HandleAnalyzeNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty() || request->ErrorCount > 0) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }
        const auto& entry = request->ResultSet.front();
        if (!entry.DomainInfo) {
            return Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, GetRequiredAccessRights())) {
            return;
        }

        if (ev->Cookie == AnalyzeSaSecondCookie) {
            if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
                SendAnalyzeListToSa(entry.DomainInfo->Params.GetStatisticsAggregator());
            } else {
                Reply(StatusIds::UNSUPPORTED, TIssuesIds::GENERIC_RESOLVE_ERROR,
                    TStringBuilder() << "No statistics aggregator found for the database "
                        << GetDatabaseName() << "; ANALYZE long-running operations are not available");
            }
            return;
        }

        const auto& domainInfo = entry.DomainInfo;
        if (!domainInfo->IsServerless()) {
            if (domainInfo->Params.HasStatisticsAggregator()) {
                SendAnalyzeListToSa(domainInfo->Params.GetStatisticsAggregator());
            } else {
                NavigateAnalyzeDomainKey(domainInfo->DomainKey);
            }
        } else {
            NavigateAnalyzeDomainKey(domainInfo->ResourcesDomainKey);
        }
    }

    void NavigateAnalyzeDomainKey(const TPathId& domainKey) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = MakeHolder<TNavigate>();
        navigate->DatabaseName = GetDatabaseName();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()),
            0, AnalyzeSaSecondCookie);
    }

    void SendAnalyzeListToSa(ui64 saTabletId) {
        if (!AnalyzePipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {.RetryLimitCount = 3};
            AnalyzePipeClient = RegisterWithSameMailbox(
                NTabletPipe::CreateClient(SelfId(), saTabletId, config));
        }
        const auto& request = *GetProtoRequest();
        NTabletPipe::SendData(SelfId(), AnalyzePipeClient,
            new NStat::TEvStatistics::TEvAnalyzeOpListRequest(
                GetDatabaseName(), request.page_size(), request.page_token()));
    }

    static constexpr ui64 AnalyzeSaCookie       = 100;
    static constexpr ui64 AnalyzeSaSecondCookie  = 101;
    TActorId AnalyzePipeClient;

    void Handle(NStat::TEvStatistics::TEvAnalyzeOpListResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvAnalyzeOpListResponse: record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        // Only populate operations when the SA reported SUCCESS. On non-success some entries may
        // still be present (e.g. partial responses); ToOperation aborts if any entry has a bad
        // OperationId, so don't iterate them on the error path.
        if (record.GetStatus() == Ydb::StatusIds::SUCCESS) {
            for (const auto& entry : record.GetEntries()) {
                auto* operation = response.add_operations();
                ::NKikimr::NGRpcService::ToOperation(entry, operation);
            }
        }
        response.set_next_page_token(record.GetNextPageToken());

        // AnalyzePipeClient is closed in PassAway (invoked by Reply), so no explicit close here.
        Reply(response);
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

    void Handle(TEvBackup::TEvListFullBackupsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvListFullBackupsResponse"
            << ": record# " << record.ShortDebugString());

        TResponse response;
        response.set_status(record.GetStatus());
        if (record.GetIssues().size()) {
            response.mutable_issues()->CopyFrom(record.GetIssues());
        }
        for (const auto& entry : record.GetEntries()) {
            *response.add_operations() = TFullBackupConv::ToOperation(entry);
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
        case TOperationId::FULL_BACKUP:
            break;
        case TOperationId::SCRIPT_EXECUTION:
            SendListScriptExecutions();
            return;
        case TOperationId::ANALYZE:
            if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR,
                    "ANALYZE long-running operation is disabled");
            }
            ResolveStatisticsAggregatorForList();
            return;

        default:
            return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Unknown operation kind");
        }

        ResolveDatabase();
    }

    void HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ParseKind(GetProtoRequest()->kind()) == TOperationId::ANALYZE) {
            HandleAnalyzeNavigateResult(ev);
        } else {
            TRpcOperationRequestActor::Handle(ev);
        }
    }

    void PassAway() override {
        // AnalyzePipeClient is opened only on the ANALYZE path. Closing the default actor id is a
        // no-op, so this is safe for every operation kind.
        NTabletPipe::CloseAndForgetClient(SelfId(), AnalyzePipeClient);
        TRpcOperationRequestActor::PassAway();
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
            hFunc(TEvBackup::TEvListFullBackupsResponse, Handle);
            hFunc(NStat::TEvStatistics::TEvAnalyzeOpListResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigateResult);
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
