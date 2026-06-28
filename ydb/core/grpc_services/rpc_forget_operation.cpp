#include "service_operation.h"
#include "operation_helpers.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_forced_compaction.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NSchemeShard;
using namespace NKikimrIssues;
using namespace NOperationId;
using namespace Ydb;

using TEvForgetOperationRequest = TGrpcRequestNoOperationCall<Ydb::Operations::ForgetOperationRequest,
    Ydb::Operations::ForgetOperationResponse>;

class TForgetOperationRPC: public TRpcOperationRequestActor<TForgetOperationRPC, TEvForgetOperationRequest> {
    TStringBuf GetLogPrefix() const override {
        switch (OperationId.GetKind()) {
        case TOperationId::EXPORT:
            return "[ForgetExport]";
        case TOperationId::IMPORT:
            return "[ForgetImport]";
        case TOperationId::BUILD_INDEX:
            return "[ForgetIndexBuild]";
        case TOperationId::SCRIPT_EXECUTION:
            return "[ForgetScriptExecution]";
        case TOperationId::INCREMENTAL_BACKUP:
            return "[ForgetIncrementalBackup]";
        case TOperationId::RESTORE:
            return "[ForgetBackupCollectionRestore]";
        case TOperationId::COMPACTION:
            return "[ForgetForcedCompaction]";
        case TOperationId::FULL_BACKUP:
            return "[ForgetFullBackup]";
        case TOperationId::ANALYZE:
            return "[ForgetAnalyze]";
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        switch (OperationId.GetKind()) {
        case TOperationId::EXPORT:
            return new TEvExport::TEvForgetExportRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::IMPORT:
            return new TEvImport::TEvForgetImportRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::BUILD_INDEX:
            return new TEvIndexBuilder::TEvForgetRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::INCREMENTAL_BACKUP:
            return new TEvBackup::TEvForgetIncrementalBackupRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::RESTORE:
            return new TEvBackup::TEvForgetBackupCollectionRestoreRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::COMPACTION:
            return new TEvForcedCompaction::TEvForgetRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::FULL_BACKUP:
            return new TEvBackup::TEvForgetFullBackupRequest(TxId, GetDatabaseName(), RawOperationId);
        default:
            Y_ABORT("unreachable");
        }
    }

    bool NeedAllocateTxId() const {
        const NOperationId::TOperationId::EKind kind = OperationId.GetKind();
        return kind == TOperationId::EXPORT
            || kind == TOperationId::IMPORT
            || kind == TOperationId::BUILD_INDEX
            || kind == TOperationId::INCREMENTAL_BACKUP
            || kind == TOperationId::RESTORE
            || kind == TOperationId::COMPACTION
            || kind == TOperationId::FULL_BACKUP;
    }

    void HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (OperationId.GetKind() == TOperationId::ANALYZE) {
            HandleSANavigateResult(ev);
        } else {
            TRpcOperationRequestActor::Handle(ev);
        }
    }

    // SA navigation for ANALYZE forget
    void ResolveStatisticsAggregatorForForget() {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto req = MakeHolder<TNavigate>();
        req->DatabaseName = GetDatabaseName();
        auto& entry = req->ResultSet.emplace_back();
        entry.Operation = TNavigate::OpPath;
        entry.Path = NKikimr::SplitPath(GetDatabaseName());
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release()), 0, SANav1Cookie);
    }

    void HandleSANavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;
        if (request->ResultSet.empty() || request->ErrorCount > 0) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR, "Scheme error");
        }
        const auto& entry = request->ResultSet.front();
        if (!entry.DomainInfo) {
            return Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR, "Internal error");
        }
        if (!this->CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, GetRequiredAccessRights())) {
            return;
        }
        if (ev->Cookie == SANav2Cookie) {
            if (!entry.DomainInfo->Params.HasStatisticsAggregator()) {
                return Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR, "No SA");
            }
            SendForgetToSA(entry.DomainInfo->Params.GetStatisticsAggregator());
            return;
        }
        const auto& domainInfo = entry.DomainInfo;
        if (!domainInfo->IsServerless()) {
            if (domainInfo->Params.HasStatisticsAggregator()) {
                SendForgetToSA(domainInfo->Params.GetStatisticsAggregator());
            } else {
                NavigateDomainKeyForSA(domainInfo->DomainKey);
            }
        } else {
            NavigateDomainKeyForSA(domainInfo->ResourcesDomainKey);
        }
    }

    void NavigateDomainKeyForSA(const TPathId& domainKey) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto nav = MakeHolder<TNavigate>();
        nav->DatabaseName = GetDatabaseName();
        auto& entry = nav->ResultSet.emplace_back();
        entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = false;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(nav.Release()), 0, SANav2Cookie);
    }

    void SendForgetToSA(ui64 saTabletId) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {.RetryLimitCount = 3};
        SAPipeClient_ = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), saTabletId, config));
        NTabletPipe::SendData(SelfId(), SAPipeClient_,
            new NStat::TEvStatistics::TEvAnalyzeOpForgetRequest(GetDatabaseName(), AnalyzeOperationId_));
    }

    void Handle(NStat::TEvStatistics::TEvAnalyzeOpForgetResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvExport::TEvForgetExportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvForgetExportResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvImport::TEvForgetImportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvForgetImportResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvIndexBuilder::TEvForgetResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvForgetResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvForcedCompaction::TEvForgetResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvForcedCompaction::TEvForgetResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(NKqp::TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issuesProto;
        NYql::IssuesToMessage(ev->Get()->Issues, &issuesProto);
        LOG_D("Handle NKqp::TEvForgetScriptExecutionOperationResponse response"
            << ": status# " << ev->Get()->Status);
        Reply(ev->Get()->Status, issuesProto);
    }

    void SendForgetScriptExecutionOperation() {
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvForgetScriptExecutionOperation(GetDatabaseName(), OperationId, GetUserSID(*Request)));
    }

    void Handle(TEvBackup::TEvForgetIncrementalBackupResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvForgetIncrementalBackupResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvBackup::TEvForgetBackupCollectionRestoreResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvForgetBackupCollectionRestoreResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvBackup::TEvForgetFullBackupResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvBackup::TEvForgetFullBackupResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void PassAway() override {
        // SAPipeClient_ is opened only on the ANALYZE path. Closing the default actor id is a no-op,
        // so this is safe regardless of operation kind.
        NTabletPipe::CloseAndForgetClient(SelfId(), SAPipeClient_);
        TRpcOperationRequestActor::PassAway();
    }

public:
    using TRpcOperationRequestActor::TRpcOperationRequestActor;

    void Bootstrap() {
        const TString& id = GetProtoRequest()->id();

        try {
            OperationId = TOperationId(id);

            switch (OperationId.GetKind()) {
            case TOperationId::EXPORT:
            case TOperationId::IMPORT:
            case TOperationId::BUILD_INDEX:
            case TOperationId::INCREMENTAL_BACKUP:
            case TOperationId::RESTORE:
            case TOperationId::COMPACTION:
            case TOperationId::FULL_BACKUP:
                if (!TryGetId(OperationId, RawOperationId)) {
                    return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Unable to extract operation id");
                }
                break;

            case TOperationId::ANALYZE:
                if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
                    return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR,
                        "ANALYZE long-running operation is disabled");
                }
                if (!TryGetUlidId(OperationId, AnalyzeOperationId_)) {
                    return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Unable to extract operation id");
                }
                ResolveStatisticsAggregatorForForget();
                Become(&TForgetOperationRPC::StateWait);
                return;

            case TOperationId::SCRIPT_EXECUTION:
                SendForgetScriptExecutionOperation();
                break;
            default:
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Unknown operation kind");
            }
            if (NeedAllocateTxId()) {
                AllocateTxId();
            }
        } catch (const yexception&) {
            return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Invalid operation id");
        }

        Become(&TForgetOperationRPC::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvForgetExportResponse, Handle);
            hFunc(TEvImport::TEvForgetImportResponse, Handle);
            hFunc(TEvIndexBuilder::TEvForgetResponse, Handle);
            hFunc(TEvForcedCompaction::TEvForgetResponse, Handle);
            hFunc(NKqp::TEvForgetScriptExecutionOperationResponse, Handle);
            hFunc(TEvBackup::TEvForgetIncrementalBackupResponse, Handle);
            hFunc(TEvBackup::TEvForgetBackupCollectionRestoreResponse, Handle);
            hFunc(TEvBackup::TEvForgetFullBackupResponse, Handle);
            hFunc(NStat::TEvStatistics::TEvAnalyzeOpForgetResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigateResult);
        default:
            return StateBase(ev);
        }
    }

private:
    TOperationId OperationId;
    ui64 RawOperationId = 0;
    TString AnalyzeOperationId_;
    TActorId SAPipeClient_;

    static constexpr ui64 SANav1Cookie = 100;
    static constexpr ui64 SANav2Cookie = 101;

}; // TForgetOperationRPC

void DoForgetOperationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TForgetOperationRPC(p.release()));
}

template<>
IActor* TEvForgetOperationRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TForgetOperationRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
