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

class TDescribeReplicationRPC: public TRpcSchemeRequestActor<TDescribeReplicationRPC, TEvDescribeReplication> {
    using TBase = TRpcSchemeRequestActor<TDescribeReplicationRPC, TEvDescribeReplication>;

public:
    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

    void PassAway() override {
        if (ControllerPipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeClient);
        }

        TBase::PassAway();
    }

private:
    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(GetProtoRequest()->path());

        Send(MakeTxProxyID(), ev.release());
        Become(&TDescribeReplicationRPC::StateDescribeScheme);
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
            Request_->RaiseIssue(issue);
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess:
                if (desc.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeReplication) {
                    auto issue = NYql::TIssue("Is not a replication");
                    Request_->RaiseIssue(issue);
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                ConvertDirectoryEntry(desc.GetSelf(), Result.mutable_self(), true);
                return DescribeReplication(desc.GetReplicationDescription().GetControllerId(),
                    PathIdFromPathId(desc.GetReplicationDescription().GetPathId()));

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);

            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);

            case NKikimrScheme::StatusNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);

            default: {
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void DescribeReplication(ui64 tabletId, const TPathId& pathId) {
        if (!ControllerPipeClient) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = {
                .RetryLimitCount = 3,
            };
            ControllerPipeClient = Register(NTabletPipe::CreateClient(SelfId(), tabletId, config));
        }

        auto ev = std::make_unique<NReplication::TEvController::TEvDescribeReplication>();
        PathIdFromPathId(pathId, ev->Record.MutablePathId());
        ev->Record.SetIncludeStats(GetProtoRequest()->include_stats());

        NTabletPipe::SendData(SelfId(), ControllerPipeClient, ev.release());
        Become(&TDescribeReplicationRPC::StateDescribeReplication);
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
            return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
        default:
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
        }

        ConvertConnectionParams(record.GetConnectionParams(), *Result.mutable_connection_params());
        ConvertState(*record.MutableState(), Result);

        for (const auto& target : record.GetTargets()) {
            ConvertItem(target, *Result.add_items());
        }

        return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
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

    static void ConvertState(NKikimrReplication::TReplicationState& from, Ydb::Replication::DescribeReplicationResult& to) {
        switch (from.GetStateCase()) {
        case NKikimrReplication::TReplicationState::kStandBy:
            to.mutable_running();
            if (from.GetStandBy().HasLagMilliSeconds()) {
                *to.mutable_running()->mutable_stats()->mutable_lag() = google::protobuf::util::TimeUtil::MillisecondsToDuration(
                    from.GetStandBy().GetLagMilliSeconds());
            }
            if (from.GetStandBy().HasInitialScanProgress()) {
                to.mutable_running()->mutable_stats()->set_initial_scan_progress(from.GetStandBy().GetInitialScanProgress());
            }
            break;
        case NKikimrReplication::TReplicationState::kError:
            *to.mutable_error()->mutable_issues() = std::move(*from.MutableError()->MutableIssues());
            break;
        case NKikimrReplication::TReplicationState::kDone:
            to.mutable_done();
            break;
        default:
            break;
        }
    }

private:
    Ydb::Replication::DescribeReplicationResult Result;
    TActorId ControllerPipeClient;
};

void DoDescribeReplication(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeReplicationRPC(p.release()));
}

}
