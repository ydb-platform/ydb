#include "service_operation.h"
#include "operation_helpers.h"
#include "rpc_operation_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/actors/core/hfunc.h>

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
        default:
            return "[Untagged]";
        }
    }

    IEventBase* MakeRequest() override {
        switch (OperationId.GetKind()) {
        case TOperationId::EXPORT:
            return new TEvExport::TEvForgetExportRequest(TxId, DatabaseName, RawOperationId);
        case TOperationId::IMPORT:
            return new TEvImport::TEvForgetImportRequest(TxId, DatabaseName, RawOperationId);
        case TOperationId::BUILD_INDEX:
            return new TEvIndexBuilder::TEvForgetRequest(TxId, DatabaseName, RawOperationId);
        default:
            Y_ABORT("unreachable");
        }
    }

    bool NeedAllocateTxId() const {
        const Ydb::TOperationId::EKind kind = OperationId.GetKind();
        return kind == TOperationId::EXPORT
            || kind == TOperationId::IMPORT
            || kind == TOperationId::BUILD_INDEX;
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

    void Handle(NKqp::TEvForgetScriptExecutionOperationResponse::TPtr& ev) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issuesProto;
        NYql::IssuesToMessage(ev->Get()->Issues, &issuesProto);
        LOG_D("Handle NKqp::TEvForgetScriptExecutionOperationResponse response"
            << ": status# " << ev->Get()->Status);
        Reply(ev->Get()->Status, issuesProto);
    }

    void SendForgetScriptExecutionOperation() {
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvForgetScriptExecutionOperation(DatabaseName, OperationId));
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
            hFunc(NKqp::TEvForgetScriptExecutionOperationResponse, Handle);
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
