#include "rpc_execute_mkql.h"
#include "service_tablet.h"

#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/grpc_services/audit_dml_operations.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/tx_proxy.pb.h>

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

namespace NKikimr::NGRpcService {


class TRpcExecuteTabletMiniKQL : public TRpcRequestActor<TRpcExecuteTabletMiniKQL, TEvExecuteTabletMiniKQLRequest> {
    using TBase = TRpcRequestActor<TRpcExecuteTabletMiniKQL, TEvExecuteTabletMiniKQLRequest>;

public:
    using TBase::TBase;

    void Bootstrap() {
        if (!CheckAccess()) {
            auto error = TStringBuilder() << "Access denied";
            if (this->UserToken) {
                error << ": '" << this->UserToken->GetUserSID() << "' is not an admin";
            }

            this->Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
            return;
        }

        auto* req = this->GetProtoRequest();
        AuditContextAppend(Request.Get(), *req);

        try {
            TabletId = req->tablet_id();
            TabletReq = std::make_unique<TEvTablet::TEvLocalMKQL>();
            auto* tx = TabletReq->Record.MutableProgram();
            tx->MutableProgram()->SetText(req->program());
            if (const auto& params = req->parameters(); !params.empty()) {
                auto* functionRegistry = AppData()->FunctionRegistry;
                NMiniKQL::TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), functionRegistry->SupportsSizedAllocators());
                NMiniKQL::TTypeEnvironment env(alloc);
                NMiniKQL::TMemoryUsageInfo memInfo("TRpcExecuteTabletMiniKQL");
                NMiniKQL::THolderFactory factory(alloc.Ref(), memInfo, functionRegistry);
                // NKikimrMiniKQL.TParams
                auto* protoParams = tx->MutableParams()->MutableProto();
                protoParams->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
                auto* protoStructType = protoParams->MutableType()->MutableStruct();
                auto* protoValue = protoParams->MutableValue();
                for (const auto& pr : params) {
                    auto* protoMember = protoStructType->AddMember();
                    protoMember->SetName(pr.first);
                    auto [pType, value] = NMiniKQL::ImportValueFromProto(pr.second.type(), pr.second.value(), env, factory);
                    ExportTypeToProto(pType, *protoMember->MutableType());
                    ExportValueToProto(pType, value, *protoValue->AddStruct());
                }
                // Tablet needs a serialized runtime node for parameters
                auto node = NMiniKQL::ImportValueFromProto(*protoParams, env);
                tx->MutableParams()->SetBin(NMiniKQL::SerializeRuntimeNode(node, env));
                // We no longer need the protobuf parameters
                tx->MutableParams()->ClearProto();
            }

            if (req->dry_run()) {
                tx->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE);
            }
        } catch (const std::exception& e) {
            this->Reply(Ydb::StatusIds::BAD_REQUEST, e.what());
            return;
        }

        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy{
            // We need at least one retry since local resolver cache may be outdated
            .RetryLimitCount = 1,
        }));

        Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup);

        Become(&TThis::StateWork);
    }

private:
    bool CheckAccess() const {
        if (AppData()->AdministrationAllowedSIDs.empty()) {
            return true;
        }

        if (!this->UserToken) {
            return false;
        }

        for (const auto& sid : AppData()->AdministrationAllowedSIDs) {
            if (this->UserToken->IsExist(sid)) {
                return true;
            }
        }

        return false;
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTablet::TEvLocalMKQLResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        auto* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            this->Reply(Ydb::StatusIds::UNAVAILABLE,
                TStringBuilder() << "Tablet " << TabletId << " is unavailable");
            return;
        }

        NTabletPipe::SendData(SelfId(), PipeClient, TabletReq.release());
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(Ydb::StatusIds::UNDETERMINED,
            TStringBuilder() << "Tablet " << TabletId << " disconnected");
    }

    void Handle(TEvTablet::TEvLocalMKQLResponse::TPtr& ev) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);

        auto* msg = ev->Get();
        auto* response = google::protobuf::Arena::CreateMessage<Ydb::Tablet::ExecuteTabletMiniKQLResponse>(Request->GetArena());

        if (msg->Record.HasCompileResults()) {
            for (const auto& issue : msg->Record.GetCompileResults().GetProgramCompileErrors()) {
                *response->add_issues() = issue;
            }
            for (const auto& issue : msg->Record.GetCompileResults().GetParamsCompileErrors()) {
                *response->add_issues() = issue;
            }
        }

        if (const TString& errors = msg->Record.GetMiniKQLErrors(); !errors.empty()) {
            auto* issue = response->add_issues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(errors);
        }

        if (msg->Record.GetStatus() != NKikimrProto::OK) {
            response->set_status(Ydb::StatusIds::GENERIC_ERROR);
            Request->Reply(response, response->status());
            return PassAway();
        }

        response->set_status(Ydb::StatusIds::SUCCESS);

        if (msg->Record.HasExecutionEngineEvaluatedResponse()) {
            try {
                const auto& protoResult = msg->Record.GetExecutionEngineEvaluatedResponse();
                auto* functionRegistry = AppData()->FunctionRegistry;
                NMiniKQL::TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), functionRegistry->SupportsSizedAllocators());
                NMiniKQL::TTypeEnvironment env(alloc);
                NMiniKQL::TMemoryUsageInfo memInfo("TRpcExecuteTabletMiniKQL");
                NMiniKQL::THolderFactory factory(alloc.Ref(), memInfo, functionRegistry);
                auto [pType, value] = NMiniKQL::ImportValueFromProto(protoResult.GetType(), protoResult.GetValue(), env, factory);
                ExportTypeToProto(pType, *response->mutable_result()->mutable_type());
                ExportValueToProto(pType, value, *response->mutable_result()->mutable_value());
            } catch (const std::exception& e) {
                response->set_status(Ydb::StatusIds::GENERIC_ERROR);
                auto* issue = response->add_issues();
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(e.what());
                Request->Reply(response, response->status());
                return PassAway();
            }
        }

        Request->Reply(response, response->status());
        return PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);
        this->Reply(Ydb::StatusIds::TIMEOUT,
            TStringBuilder() << "Tablet " << TabletId << " is not responding");
    }

private:
    ui64 TabletId;
    std::unique_ptr<TEvTablet::TEvLocalMKQL> TabletReq;
    TActorId PipeClient;
};

void DoExecuteTabletMiniKQLRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRpcExecuteTabletMiniKQL(p.release()));
}

template<>
IActor* TEvExecuteTabletMiniKQLRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestNoOpCtx* msg) {
    return new TRpcExecuteTabletMiniKQL(msg);
}

} // namespace NKikimr::NGRpcService
