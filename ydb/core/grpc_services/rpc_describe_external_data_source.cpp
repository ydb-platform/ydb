#include "rpc_scheme_base.h"
#include "service_table.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace NKikimrSchemeOp;
using namespace NYql;

namespace {

using TProperties = google::protobuf::Map<TProtoStringType, TProtoStringType>;

void Convert(const TServiceAccountAuth& in, TProperties& out) {
    out["SERVICE_ACCOUNT_ID"] = in.GetId();
    out["SERVICE_ACCOUNT_SECRET_NAME"] = in.GetSecretName();
}

void Convert(const TBasic& in, TProperties& out) {
    out["LOGIN"] = in.GetLogin();
    out["PASSWORD_SECRET_NAME"] = in.GetPasswordSecretName();
}

void Convert(const TMdbBasic& in, TProperties& out) {
    out["SERVICE_ACCOUNT_ID"] = in.GetServiceAccountId();
    out["SERVICE_ACCOUNT_SECRET_NAME"] = in.GetServiceAccountSecretName();
    out["LOGIN"] = in.GetLogin();
    out["PASSWORD_SECRET_NAME"] = in.GetPasswordSecretName();
}

void Convert(const TAws& in, TProperties& out) {
    out["AWS_ACCESS_KEY_ID_SECRET_NAME"] = in.GetAwsAccessKeyIdSecretName();
    out["AWS_SECRET_ACCESS_KEY_SECRET_NAME"] = in.GetAwsSecretAccessKeySecretName();
    out["AWS_REGION"] = in.GetAwsRegion();
}

void Convert(const TToken& in, TProperties& out) {
    out["TOKEN_SECRET_NAME"] = in.GetTokenSecretName();
}

void Convert(const TAuth& in, TProperties& out) {
    auto& authMethod = out["AUTH_METHOD"];

    switch (in.GetIdentityCase()) {
    case TAuth::kNone:
        authMethod = "NONE";
        return;
    case TAuth::kServiceAccount:
        authMethod = "SERVICE_ACCOUNT";
        Convert(in.GetServiceAccount(), out);
        return;
    case TAuth::kBasic:
        authMethod = "BASIC";
        Convert(in.GetBasic(), out);
        return;
    case TAuth::kMdbBasic:
        authMethod = "MDB_BASIC";
        Convert(in.GetMdbBasic(), out);
        return;
    case TAuth::kAws:
        authMethod = "AWS";
        Convert(in.GetAws(), out);
        return;
    case TAuth::kToken:
        authMethod = "TOKEN";
        Convert(in.GetToken(), out);
        return;
    case TAuth::IDENTITY_NOT_SET:
        return;
    }
}

Ydb::Table::DescribeExternalDataSourceResult Convert(const TDirEntry& inSelf, const TExternalDataSourceDescription& inDesc) {
    Ydb::Table::DescribeExternalDataSourceResult out;
    ConvertDirectoryEntry(inSelf, out.mutable_self(), true);

    out.set_source_type(inDesc.GetSourceType());
    out.set_location(inDesc.GetLocation());
    auto& properties = *out.mutable_properties();
    for (const auto& [key, value] : inDesc.GetProperties().GetProperties()) {
        properties[to_upper(key)] = value;
    }
    Convert(inDesc.GetAuth(), properties);
    return out;
}

}

using TEvDescribeExternalDataSourceRequest = TGrpcRequestOperationCall<
    Ydb::Table::DescribeExternalDataSourceRequest,
    Ydb::Table::DescribeExternalDataSourceResponse
>;

class TDescribeExternalDataSourceRPC : public TRpcSchemeRequestActor<TDescribeExternalDataSourceRPC, TEvDescribeExternalDataSourceRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeExternalDataSourceRPC, TEvDescribeExternalDataSourceRequest>;

public:

    using TBase::TBase;

    void Bootstrap() {
        DescribeScheme();
    }

private:

    void DescribeScheme() {
        auto ev = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev.get(), *Request_);
        ev->Record.MutableDescribePath()->SetPath(GetProtoRequest()->path());

        Send(MakeTxProxyID(), ev.release());
        Become(&TDescribeExternalDataSourceRPC::StateDescribeScheme);
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
        const auto& pathDescription = record.GetPathDescription();

        if (record.HasReason()) {
            Request_->RaiseIssue(TIssue(record.GetReason()));
        }

        switch (record.GetStatus()) {
            case NKikimrScheme::StatusSuccess: {
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeExternalDataSource) {
                    Request_->RaiseIssue(TIssue(
                        TStringBuilder() << "Unexpected path type: " << pathDescription.GetSelf().GetPathType()
                    ));
                    return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }

                return ReplyWithResult(
                    Ydb::StatusIds::SUCCESS,
                    Convert(pathDescription.GetSelf(), pathDescription.GetExternalDataSourceDescription()),
                    ctx
                );
            }
            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError:
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);

            case NKikimrScheme::StatusAccessDenied:
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);

            case NKikimrScheme::StatusNotAvailable:
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);

            default:
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
        }
    }
};

void DoDescribeExternalDataSourceRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeExternalDataSourceRPC(p.release()));
}

}
