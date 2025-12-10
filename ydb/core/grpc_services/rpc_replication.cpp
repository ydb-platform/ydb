#include "rpc_scheme_base.h"
#include "service_replication.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/builder.h>

namespace NKikimr::NGRpcService {

using namespace Ydb;

using TEvDescribeReplication = TGrpcRequestOperationCall<Replication::DescribeReplicationRequest, Replication::DescribeReplicationResponse>;
using TEvDescribeTransfer = TGrpcRequestOperationCall<Replication::DescribeTransferRequest, Replication::DescribeTransferResponse>;

template <typename TReq, typename TResp, typename TResult>
class TDescribeReplicationRPC: public TRpcSchemeRequestActor<TDescribeReplicationRPC<TReq, TResp, TResult>, TGrpcRequestOperationCall<TReq, TResp>> {
    using TBase = TRpcSchemeRequestActor<TDescribeReplicationRPC<TReq, TResp, TResult>, TGrpcRequestOperationCall<TReq, TResp>>;
    using TThis = TDescribeReplicationRPC<TReq, TResp, TResult>;

public:
    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

    void PassAway() override {
        if (ControllerPipeClient) {
            NTabletPipe::CloseAndForgetClient(TBase::SelfId(), ControllerPipeClient);
        }

        TBase::PassAway();
    }

private:
    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *TBase::Request_);
        SetDatabase(ev.get(), *TBase::Request_);
        ev->Record.MutableDescribePath()->SetPath(TBase::GetProtoRequest()->path());

        TBase::Send(MakeTxProxyID(), ev.release());
        TBase::Become(&TThis::StateDescribeScheme);
    }

    STATEFN(StateDescribeScheme) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            sFunc(TEvents::TEvWakeup, DescribeScheme);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto& desc = record.GetPathDescription();

        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            TBase::Request_->RaiseIssue(issue);
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess:
                switch (desc.GetSelf().GetPathType()) {
                    case NKikimrSchemeOp::EPathTypeReplication:
                    case NKikimrSchemeOp::EPathTypeTransfer:
                        break;
                    default: {
                        auto issue = NYql::TIssue("Is not a replication");
                        TBase::Request_->RaiseIssue(issue);
                        return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                    }
                }

                if (desc.GetReplicationDescription().GetControllerId() == 0) {
                    if (Backoff.HasMore()) {
                        return ctx.Schedule(Backoff.Next(), new TEvents::TEvWakeup());
                    } else {
                        auto issue = NYql::TIssue("The creation of the object has not been completed yet");
                        TBase::Request_->RaiseIssue(issue);
                        return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                    }
                }

                ConvertDirectoryEntry(desc.GetSelf(), Result.mutable_self(), true);
                return DescribeReplication(desc.GetReplicationDescription().GetControllerId(),
                    TPathId::FromProto(desc.GetReplicationDescription().GetPathId()));

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);

            case NKikimrScheme::StatusAccessDenied:
                return TBase::Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);

            case NKikimrScheme::StatusNotAvailable:
                return TBase::Reply(Ydb::StatusIds::UNAVAILABLE, ctx);

            default: {
                return TBase::Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void DescribeReplication(ui64 tabletId, const TPathId& pathId) {
        if (!ControllerPipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {
                .RetryLimitCount = 3,
            };
            ControllerPipeClient = TBase::Register(NTabletPipe::CreateClient(TBase::SelfId(), tabletId, config));
        }

        auto ev = std::make_unique<NReplication::TEvController::TEvDescribeReplication>();
        pathId.ToProto(ev->Record.MutablePathId());
        BuildRequest(TBase::GetProtoRequest(), ev->Record);

        NTabletPipe::SendData(TBase::SelfId(), ControllerPipeClient, ev.release());
        TBase::Become(&TThis::StateDescribeReplication);
    }

    STATEFN(StateDescribeReplication) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NReplication::TEvController::TEvDescribeReplicationResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(NReplication::TEvController::TEvDescribeReplicationResult::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrReplication::TEvDescribeReplicationResult::SUCCESS:
            break;
        case NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND:
            return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        default:
            return TBase::Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        Convert(record, Result);

        return TBase::ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
    }

    static void Convert(NKikimrReplication::TEvDescribeReplicationResult& record, Replication::DescribeReplicationResult& result) {
        FillReplicationDescription(result, record);
    }

    static void Convert(NKikimrReplication::TEvDescribeReplicationResult& record, Replication::DescribeTransferResult& result) {
<<<<<<< HEAD
        FillTransferDescription(result, record);
=======
        ConvertConnectionParams(record.GetConnectionParams(), *result.mutable_connection_params());
        ConvertState(*record.MutableState(), result);
        if (record.HasStats()&& record.GetStats().HasTransfer()) {
            result.mutable_stats()->CopyFrom(record.GetStats().GetTransfer());
        }

        const auto& transferSpecific = record.GetTransferSpecific();
        result.set_source_path(transferSpecific.GetTarget().GetSrcPath());
        result.set_destination_path(transferSpecific.GetTarget().GetDstPath());
        result.set_consumer_name(transferSpecific.GetTarget().GetConsumerName());
        result.set_transformation_lambda(transferSpecific.GetTarget().GetTransformLambda());
        result.mutable_batch_settings()->set_size_bytes(transferSpecific.GetBatching().GetBatchSizeBytes());
        result.mutable_batch_settings()->mutable_flush_interval()->set_seconds(transferSpecific.GetBatching().GetFlushIntervalMilliSeconds() / 1000);
>>>>>>> ca26d076cb3 (Functional, w/o tests)
    }

    static void BuildRequest(const Replication::DescribeReplicationRequest* from, NKikimrReplication::TEvDescribeReplication& to) {
        to.SetIncludeStats(from->include_stats());
    }

    static void BuildRequest(const Replication::DescribeTransferRequest* from, NKikimrReplication::TEvDescribeReplication& to) {
        to.SetIncludeStats(from->include_stats());
    }

private:
    TResult Result;
    TActorId ControllerPipeClient;
    TBackoff Backoff = TBackoff(5, TDuration::MilliSeconds(100));
};

using TDescribeReplicationActor = TDescribeReplicationRPC<Replication::DescribeReplicationRequest, Replication::DescribeReplicationResponse, Replication::DescribeReplicationResult>;
using TDescribeTransferActor = TDescribeReplicationRPC<Replication::DescribeTransferRequest, Replication::DescribeTransferResponse, Replication::DescribeTransferResult>;

void DoDescribeReplication(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeReplicationActor(p.release()));
}

void DoDescribeTransfer(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeTransferActor(p.release()));
}

using TEvDescribeReplicationRequest = TGrpcRequestOperationCall<Ydb::Replication::DescribeReplicationRequest, Ydb::Replication::DescribeReplicationResponse>;

template<>
IActor* TEvDescribeReplicationRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TDescribeReplicationActor(msg);
}

using TEvDescribeTransferRequest = TGrpcRequestOperationCall<Ydb::Replication::DescribeTransferRequest, Ydb::Replication::DescribeTransferResponse>;

template<>
IActor* TEvDescribeTransferRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TDescribeTransferActor(msg);
}

}
