#include "service_operation.h"
#include "operation_helpers.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
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
            || kind == TOperationId::COMPACTION;
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
                if (!TryGetId(OperationId, RawOperationId)) {
                    return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Unable to extract operation id");
                }
                break;

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
        default:
            return StateBase(ev);
        }
    }

private:
    TOperationId OperationId;
    ui64 RawOperationId = 0;

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
