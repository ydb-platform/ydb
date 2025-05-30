#include "grpc_request_proxy.h"
#include "rpc_calls.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/actorlib_impl/long_timer.h>

namespace NKikimr {
namespace NGRpcService {

using TEvBsControllerDescribeRequest = TGrpcRequestOperationCall<Ydb::BsController::DescribeRequest, Ydb::BsController::DescribeResponse>;

class TBsControllerDescribeRequestGrpc : public TActorBootstrapped<TBsControllerDescribeRequestGrpc> {
private:
    typedef TActorBootstrapped<TThis> TBase;

    static constexpr ui32 DEFAULT_TIMEOUT_SEC = 5*60;

    std::unique_ptr<IRequestNoOpCtx> GrpcRequest;
    const Ydb::BsController::DescribeRequest* Request;
    TDuration Timeout;
    bool Finished;
    TActorId BscPipe;
    TActorId TimeoutTimerActorId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TBsControllerDescribeRequestGrpc(std::unique_ptr<IRequestNoOpCtx> request)
        : GrpcRequest(std::move(request))
        , Request(TEvBsControllerDescribeRequest::GetProtoRequest(GrpcRequest.get()))
        , Timeout(TDuration::Seconds(DEFAULT_TIMEOUT_SEC))
        , Finished(false)
    {
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString errDescr;
        if (!Request) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errDescr, ctx);
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        auto pipe = NTabletPipe::CreateClient(ctx.SelfID, MakeBSControllerID(), pipeConfig);
        BscPipe = ctx.RegisterWithSameMailbox(pipe);

        auto bscReq = MakeHolder<TEvBlobStorage::TEvControllerDescribeRequest>();

        auto& record = bscReq->Record;

        for (auto& target : Request->get_arr_targets()) {
            switch (target.GetTargetCase()) {
                case Ydb::BsController::DescribeRequest_TargetWrapper::kVdiskId: {
                    auto& vdiskId = target.vdisk_id();
                    auto* mutableVdiskId = record.add_targets()->mutable_vdiskid();
                    mutableVdiskId->set_groupid(vdiskId.group_id());
                    mutableVdiskId->set_groupgeneration(vdiskId.group_generation());
                    mutableVdiskId->set_ring(vdiskId.ring());
                    mutableVdiskId->set_domain(vdiskId.domain());
                    mutableVdiskId->set_vdisk(vdiskId.vdisk());
                    break;
                }
                case Ydb::BsController::DescribeRequest_TargetWrapper::kPdiskId: {
                    auto& pdiskId = target.pdisk_id();
                    auto* mutablePdiskId = record.add_targets()->mutable_pdiskid();
                    mutablePdiskId->set_nodeid(pdiskId.node_id());
                    mutablePdiskId->set_pdiskid(pdiskId.pdisk_id()); 
                    break;
                }
                case Ydb::BsController::DescribeRequest_TargetWrapper::kGroupId: {
                    record.add_targets()->SetGroupId(target.group_id());
                    break;
                }
                default:
                    break;
            }
        }

        NTabletPipe::SendData(ctx, BscPipe, bscReq.Release());

        TBase::Become(&TThis::StateFunc);
    }

    void Die(const NActors::TActorContext& ctx) override {
        Y_VERIFY(Finished);
        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        NTabletPipe::CloseClient(ctx, BscPipe);
        TBase::Die(ctx);
    }

private:

    void Handle(TEvBlobStorage::TEvControllerDescribeResponse::TPtr &ev, const TActorContext &ctx) {
        auto* resp = CreateResponse();
        resp->set_status(Ydb::StatusIds::SUCCESS);

        for (auto& bscResult : ev->Get()->Record.GetResults()) {
            auto* result = resp->add_results();
            
            switch (bscResult.GetResponseCase()) {
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kVDiskResponse: {
                    result->mutable_vdisk_response()->set_result(bscResult.GetVDiskResponse().GetResult());
                    break;
                }
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kPDiskResponse: {
                    result->mutable_pdisk_response()->set_result(bscResult.GetPDiskResponse().GetResult());
                    break;
                }
                case NKikimrBlobStorage::TEvControllerDescribeResponse_TResponse::kGroupResponse: {
                    result->mutable_group_response()->set_result(bscResult.GetGroupResponse().GetResult());
                    break;
                }
                default:
                    break;
            }
        }

        try {
            GrpcRequest->Reply(resp, Ydb::StatusIds::SUCCESS);
        } catch(std::exception ex) {
            GrpcRequest->RaiseIssue(NYql::ExceptionToIssue(ex));
            GrpcRequest->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
        }
        
        Finished = true;
        Die(ctx);
    }

    Ydb::BsController::DescribeResponse* CreateResponse() {
        return google::protobuf::Arena::CreateMessage<Ydb::BsController::DescribeResponse>(Request->GetArena());
    }

    void ReplyWithError(Ydb::StatusIds::StatusCode grpcStatus, const TString& message, const TActorContext& ctx) {
        auto* resp = CreateResponse();
        resp->set_status(grpcStatus);

        if (!message.empty()) {
            const NYql::TIssue& issue = NYql::TIssue(message);
            auto* protoIssue = resp->add_issues();
            NYql::IssueToMessage(issue, protoIssue);
        }

        GrpcRequest->Reply(resp, grpcStatus);

        Finished = true;

        Die(ctx);
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, "Request timed out", ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR,
                       "Internal error: pipe cache is not available, the cluster might not be configured properly", ctx);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ReplyWithError(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to BlobStorageController", ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerDescribeResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }
};

IActor* CreateGrpcBsControllerDescribeHandler(std::unique_ptr<IRequestNoOpCtx> request) {
    return new TBsControllerDescribeRequestGrpc(std::move(request));
}

void DoBsControllerDescribeRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f){
    f.RegisterActor(CreateGrpcBsControllerDescribeHandler(std::move(p)));
}

} // namespace NKikimr
} // namespace NGRpcService
