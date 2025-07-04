#include "rpc_scheme_base.h"
#include "service_replication.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
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

    static TString BuildConnectionString(const NKikimrReplication::TConnectionParams& params) {
        return TStringBuilder()
            << (params.GetEnableSsl() ? "grpcs://" : "grpc://")
            << params.GetEndpoint()
            << "/?database=" << params.GetDatabase();
    }

    static void ConvertConnectionParams(const NKikimrReplication::TConnectionParams& from, Ydb::Replication::ConnectionParams& to) {
        to.set_endpoint(from.GetEndpoint());
        to.set_database(from.GetDatabase());
        to.set_enable_ssl(from.GetEnableSsl());
        to.set_connection_string(BuildConnectionString(from));

        switch (from.GetCredentialsCase()) {
        case NKikimrReplication::TConnectionParams::kStaticCredentials:
            return ConvertStaticCredentials(from.GetStaticCredentials(), *to.mutable_static_credentials());
        case NKikimrReplication::TConnectionParams::kOAuthToken:
            return ConvertOAuth(from.GetOAuthToken(), *to.mutable_oauth());
        default:
            break;
        }
    }

    static void ConvertStaticCredentials(const NKikimrReplication::TStaticCredentials& from, Ydb::Replication::ConnectionParams::StaticCredentials& to) {
        to.set_user(from.GetUser());
        to.set_password_secret_name(from.GetPasswordSecretName());
    }

    static void ConvertOAuth(const NKikimrReplication::TOAuthToken& from, Ydb::Replication::ConnectionParams::OAuth& to) {
        to.set_token_secret_name(from.GetTokenSecretName());
    }

    static void ConvertConsistencySettings(const NKikimrReplication::TConsistencySettings& from, Ydb::Replication::DescribeReplicationResult& to) {
        switch (from.GetLevelCase()) {
        case NKikimrReplication::TConsistencySettings::kRow:
            return ConvertRowConsistencySettings(from.GetRow(), *to.mutable_row_consistency());
        case NKikimrReplication::TConsistencySettings::kGlobal:
            return ConvertGlobalConsistencySettings(from.GetGlobal(), *to.mutable_global_consistency());
        default:
            break;
        }
    }

    static void ConvertRowConsistencySettings(const NKikimrReplication::TConsistencySettings::TRowConsistency&, Ydb::Replication::ConsistencyLevelRow&) {
        // nop
    }

    static void ConvertGlobalConsistencySettings(const NKikimrReplication::TConsistencySettings::TGlobalConsistency& from, Ydb::Replication::ConsistencyLevelGlobal& to) {
        *to.mutable_commit_interval() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
            from.GetCommitIntervalMilliSeconds());
    }

    static void ConvertItem(const NKikimrReplication::TReplicationConfig::TTargetSpecific::TTarget& from, Ydb::Replication::DescribeReplicationResult::Item& to) {
        to.set_id(from.GetId());
        to.set_source_path(from.GetSrcPath());
        to.set_destination_path(from.GetDstPath());
        if (from.HasSrcStreamName()) {
            to.set_source_changefeed_name(from.GetSrcStreamName());
        }
        if (from.HasLagMilliSeconds()) {
            *to.mutable_stats()->mutable_lag() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
                from.GetLagMilliSeconds());
        }
        if (from.HasInitialScanProgress()) {
            to.mutable_stats()->set_initial_scan_progress(from.GetInitialScanProgress());
        }
    }

    static void ConvertStats(NKikimrReplication::TReplicationState& from, Ydb::Replication::DescribeReplicationResult& to) {
        if (from.GetStandBy().HasLagMilliSeconds()) {
            *to.mutable_running()->mutable_stats()->mutable_lag() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
                from.GetStandBy().GetLagMilliSeconds());
        }
        if (from.GetStandBy().HasInitialScanProgress()) {
            to.mutable_running()->mutable_stats()->set_initial_scan_progress(from.GetStandBy().GetInitialScanProgress());
        }
    }

    static void ConvertStats(NKikimrReplication::TReplicationState&, Ydb::Replication::DescribeTransferResult&) {
        // nop
    }

    template<typename T>
    static void ConvertState(NKikimrReplication::TReplicationState& from, T& to) {
        switch (from.GetStateCase()) {
        case NKikimrReplication::TReplicationState::kStandBy:
            to.mutable_running();
            ConvertStats(from, to);
            break;
        case NKikimrReplication::TReplicationState::kError:
            *to.mutable_error()->mutable_issues() = std::move(*from.MutableError()->MutableIssues());
            break;
        case NKikimrReplication::TReplicationState::kDone:
            to.mutable_done();
            break;
        case NKikimrReplication::TReplicationState::kPaused:
            to.mutable_paused();
            break;
        default:
            break;
        }
    }

    static void Convert(NKikimrReplication::TEvDescribeReplicationResult& record, Replication::DescribeReplicationResult& result) {
        ConvertConnectionParams(record.GetConnectionParams(), *result.mutable_connection_params());
        ConvertConsistencySettings(record.GetConsistencySettings(), result);
        ConvertState(*record.MutableState(), result);

        for (const auto& target : record.GetTargets()) {
            ConvertItem(target, *result.add_items());
        }
    }

    static void Convert(NKikimrReplication::TEvDescribeReplicationResult& record, Replication::DescribeTransferResult& result) {
        ConvertConnectionParams(record.GetConnectionParams(), *result.mutable_connection_params());
        ConvertState(*record.MutableState(), result);

        const auto& transferSpecific = record.GetTransferSpecific();
        result.set_source_path(transferSpecific.GetTarget().GetSrcPath());
        result.set_destination_path(transferSpecific.GetTarget().GetDstPath());
        result.set_consumer_name(transferSpecific.GetTarget().GetConsumerName());
        result.set_transformation_lambda(transferSpecific.GetTarget().GetTransformLambda());
        result.mutable_batch_settings()->set_size_bytes(transferSpecific.GetBatching().GetBatchSizeBytes());
        result.mutable_batch_settings()->mutable_flush_interval()->set_seconds(transferSpecific.GetBatching().GetFlushIntervalMilliSeconds() / 1000);
    }

    static void BuildRequest(const Replication::DescribeReplicationRequest* from, NKikimrReplication::TEvDescribeReplication& to) {
        to.SetIncludeStats(from->include_stats());
    }

    static void BuildRequest(const Replication::DescribeTransferRequest*, NKikimrReplication::TEvDescribeReplication&) {
        // nop
    }

private:
    TResult Result;
    TActorId ControllerPipeClient;
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
