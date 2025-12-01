#include "rpc_test_shard_base.h"

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NGRpcService {

using namespace NActors;

using TEvCreateTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::CreateTestShardRequest,
        Ydb::TestShard::CreateTestShardResponse>;
using TEvDeleteTestShardRequest =
    TGrpcRequestOperationCall<Ydb::TestShard::DeleteTestShardRequest,
        Ydb::TestShard::DeleteTestShardResponse>;

class TCreateTestShardRequest : public TRpcSchemeRequestActor<TCreateTestShardRequest, TEvCreateTestShardRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TCreateTestShardRequest, TEvCreateTestShardRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TCreateTestShardRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTestShard);
        auto* op = modifyScheme->MutableCreateTestShard();
        
        op->SetName(name);
        op->SetCount(req->count());
        op->SetConfig(req->config());

        auto* storageConfig = op->MutableStorageConfig();
        for (const auto& channel : req->channels()) {
             auto* settings = storageConfig->AddChannel();
             settings->SetPreferredPoolKind(channel);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};

class TDeleteTestShardRequest : public TRpcSchemeRequestActor<TDeleteTestShardRequest, TEvDeleteTestShardRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TDeleteTestShardRequest, TEvDeleteTestShardRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TDeleteTestShardRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTestShard);
        auto* op = modifyScheme->MutableDrop();
        
        op->SetName(name);

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};

void DoCreateTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateTestShardRequest(p.release()));
}

void DoDeleteTestShard(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDeleteTestShardRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
