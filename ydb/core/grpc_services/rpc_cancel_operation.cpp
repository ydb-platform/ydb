#include "service_operation.h"
#include "operation_helpers.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_forced_compaction.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
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

using TEvCancelOperationRequest = TGrpcRequestNoOperationCall<Ydb::Operations::CancelOperationRequest,
    Ydb::Operations::CancelOperationResponse>;

class TCancelOperationRPC: public TRpcOperationRequestActor<TCancelOperationRPC, TEvCancelOperationRequest> {
    TStringBuf GetLogPrefix() const override {
        switch (OperationId.GetKind()) {
        case TOperationId::EXPORT:
            return "[CancelExport]";
        case TOperationId::IMPORT:
            return "[CancelImport]";
        case TOperationId::BUILD_INDEX:
            return "[CancelIndexBuild]";
        case TOperationId::SCRIPT_EXECUTION:
            return "[CancelScriptExecution]";
        case TOperationId::INCREMENTAL_BACKUP:
            return "[CancelIncrementalBackup]";
        case TOperationId::RESTORE:
            return "[CancelBackupCollectionRestore]";
        case TOperationId::COMPACTION:
            return "[CancelForcedCompaction]";
        case TOperationId::ANALYZE:
            return "[CancelAnalyze]";
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        switch (OperationId.GetKind()) {
        case TOperationId::EXPORT:
            return new TEvExport::TEvCancelExportRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::IMPORT:
            return new TEvImport::TEvCancelImportRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::BUILD_INDEX:
            return new TEvIndexBuilder::TEvCancelRequest(TxId, GetDatabaseName(), RawOperationId);
        case TOperationId::COMPACTION:
            return new TEvForcedCompaction::TEvCancelRequest(TxId, GetDatabaseName(), RawOperationId);
        default:
            Y_ABORT("unreachable");
        }
    }

    bool NeedAllocateTxId() const {
        const TOperationId::EKind kind = OperationId.GetKind();
        return kind == TOperationId::EXPORT
            || kind == TOperationId::IMPORT
            || kind == TOperationId::BUILD_INDEX
            || kind == TOperationId::COMPACTION;
    }

    void HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (OperationId.GetKind() == TOperationId::ANALYZE) {
            HandleSANavigateResult(ev);
        } else {
            TRpcOperationRequestActor::Handle(ev);
        }
    }

    // SA-specific cancel navigation
    void ResolveStatisticsAggregatorForCancel() {
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
            SendCancelToSA(entry.DomainInfo->Params.GetStatisticsAggregator());
            return;
        }
        const auto& domainInfo = entry.DomainInfo;
        if (!domainInfo->IsServerless()) {
            if (domainInfo->Params.HasStatisticsAggregator()) {
                SendCancelToSA(domainInfo->Params.GetStatisticsAggregator());
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

    void SendCancelToSA(ui64 saTabletId) {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {.RetryLimitCount = 3};
        SAPipeClient_ = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), saTabletId, config));
        NTabletPipe::SendData(SelfId(), SAPipeClient_,
            new NStat::TEvStatistics::TEvAnalyzeOpCancelRequest(GetDatabaseName(), AnalyzeOperationId_));
    }

    void Handle(NStat::TEvStatistics::TEvAnalyzeOpCancelResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvExport::TEvCancelExportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvExport::TEvCancelExportResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvImport::TEvCancelImportResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvImport::TEvCancelImportResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvIndexBuilder::TEvCancelResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvIndexBuilder::TEvCancelResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
    }

    void Handle(TEvForcedCompaction::TEvCancelResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvForcedCompaction::TEvCancelResponse"
            << ": record# " << record.ShortDebugString());

        Reply(record.GetStatus(), record.GetIssues());
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
            case TOperationId::COMPACTION:
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
                ResolveStatisticsAggregatorForCancel();
                Become(&TCancelOperationRPC::StateWait);
                return;

            case TOperationId::SCRIPT_EXECUTION:
                SendCancelScriptExecutionOperation();
                break;

            case TOperationId::INCREMENTAL_BACKUP:
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Cancel isn't supported for incremental backup yet");

            case TOperationId::RESTORE:
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Cancel isn't supported for incremental restore yet");

            case TOperationId::FULL_BACKUP:
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Cancel isn't supported for full backup yet");

            default:
                return Reply(StatusIds::UNSUPPORTED, TIssuesIds::DEFAULT_ERROR, "Unknown operation kind");
            }

            if (NeedAllocateTxId()) {
                AllocateTxId();
            }
        } catch (const yexception&) {
            return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Invalid operation id");
        }

        Become(&TCancelOperationRPC::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvCancelExportResponse, Handle);
            hFunc(TEvImport::TEvCancelImportResponse, Handle);
            hFunc(TEvIndexBuilder::TEvCancelResponse, Handle);
            hFunc(TEvForcedCompaction::TEvCancelResponse, Handle);
            hFunc(NKqp::TEvCancelScriptExecutionOperationResponse, Handle);
            hFunc(NStat::TEvStatistics::TEvAnalyzeOpCancelResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigateResult);
        default:
            return StateBase(ev);
        }
    }

    void Handle(NKqp::TEvCancelScriptExecutionOperationResponse::TPtr& ev) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issuesProto;
        NYql::IssuesToMessage(ev->Get()->Issues, &issuesProto);
        Reply(ev->Get()->Status, issuesProto);
    }

    void SendCancelScriptExecutionOperation() {
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvCancelScriptExecutionOperation(GetDatabaseName(), OperationId, GetUserSID(*Request)));
    }

    void PassAway() override {
        // SAPipeClient_ is opened only on the ANALYZE path. Closing the default actor id is a no-op,
        // so this is safe regardless of operation kind.
        NTabletPipe::CloseAndForgetClient(SelfId(), SAPipeClient_);
        TRpcOperationRequestActor::PassAway();
    }

private:
    TOperationId OperationId;
    ui64 RawOperationId = 0;
    TString AnalyzeOperationId_;
    TActorId SAPipeClient_;

    static constexpr ui64 SANav1Cookie = 100;
    static constexpr ui64 SANav2Cookie = 101;
}; // TCancelOperationRPC

void DoCancelOperationRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCancelOperationRPC(p.release()));
}

template<>
IActor* TEvCancelOperationRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TCancelOperationRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
