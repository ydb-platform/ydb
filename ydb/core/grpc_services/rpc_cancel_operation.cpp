#include "service_operation.h"
#include "operation_helpers.h"
#include "rpc_operation_request_base.h"
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/actors/core/hfunc.h>

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
                SendCancelScriptExecutionOperation();
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

        Become(&TCancelOperationRPC::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExport::TEvCancelExportResponse, Handle);
            hFunc(TEvImport::TEvCancelImportResponse, Handle);
            hFunc(TEvIndexBuilder::TEvCancelResponse, Handle);
            hFunc(NKqp::TEvCancelScriptExecutionOperationResponse, Handle);
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
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), new NKqp::TEvCancelScriptExecutionOperation(GetDatabaseName(), OperationId));
    }

private:
    TOperationId OperationId;
    ui64 RawOperationId = 0;
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
