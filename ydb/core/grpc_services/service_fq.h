#pragma once

#include <algorithm>
#include <memory>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/utils.h>

namespace NKikimr {
namespace NGRpcService {

namespace NFederatedQuery {

inline TMaybe<TString> GetYdbToken(const IRequestCtx& req) {
    return req.GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER);
}

template <typename TReq>
using TPermissionsFunc = std::function<TVector<NKikimr::TEvTicketParser::TEvAuthorizeTicket::TPermission>(const TReq&)>;
}

class IRequestOpCtx;
class IFacilityProvider;


template <typename TReq>
class TFqPermissionsBase {
public:
    using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;
    using TPermissionsFunc = NFederatedQuery::TPermissionsFunc<TReq>;

    TFqPermissionsBase(TPermissionsFunc&& permissions) noexcept
        : Permissions{std::move(permissions)}
    {}

    const TVector<TString>& GetSids() const noexcept {
        return Sids;
    }

    TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> FillSids(const TString& scope, const TReq& req) {
        if (!scope.StartsWith("yandexcloud://")) {
            return {};
        }

        const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
        if (path.size() != 2 && path.size() != 3) {
            return {};
        }

        const TString& folderId = path.back();
        const auto& permissions = Permissions(req);
        TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
            permissions,
            {
                {"folder_id", folderId}
            }
        }};
        std::transform(permissions.begin(), permissions.end(), std::back_inserter(Sids),
            [](const auto& s) -> TString { return s.Permission + "@as"; });

        auto serviceAccountId = NFq::ExtractServiceAccountId(req);
        if (serviceAccountId) {
            entries.push_back({
                {{NPerms::Required("iam.serviceAccounts.use")}},
                {
                    {"service_account_id", serviceAccountId}
                }});
            Sids.push_back("iam.serviceAccounts.use@as");
        }

        return entries;
    }
private:
    TVector<TString> Sids;
    TPermissionsFunc Permissions;
};

template <typename TReq, typename TResp>
class TGrpcFqRequestOperationCall : public TGrpcRequestOperationCall<TReq, TResp>, public TFqPermissionsBase<TReq> {
public:
    using TBase = TGrpcRequestOperationCall<TReq, TResp>;
    using TBase::GetProtoRequest;
    using TBase::GetPeerMetaValues;
    using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;
    using TPermissions = TFqPermissionsBase<TReq>;

    // const std::function<TVector<NPerms::TPermission>(const TReq&)>& Permissions;
    // TVector<TString> Sids;

    TGrpcFqRequestOperationCall(NYdbGrpc::IRequestContextBase* ctx,
        void (*cb)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider& f),
        std::function<TVector<NPerms::TPermission>(const TReq&)>&& permissions)
        : TGrpcRequestOperationCall<TReq, TResp>(ctx, cb, {}), TFqPermissionsBase<TReq>(std::move(permissions)) {
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& , ICheckerIface* iface) override {
        TString scope = GetPeerMetaValues("x-ydb-fq-project").GetOrElse("");
        TVector entries = TPermissions::FillSids(scope, *GetProtoRequest());
        if (entries.empty()) {
            return false;
        }

        iface->SetEntries(entries);
        return true;
    }
};

void DoFederatedQueryCreateQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoFederatedQueryDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateQueryRequest>        GetFederatedQueryCreateQueryPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ListQueriesRequest>        GetFederatedQueryListQueriesPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeQueryRequest>      GetFederatedQueryDescribeQueryPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::GetQueryStatusRequest>     GetFederatedQueryGetQueryStatusPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyQueryRequest>        GetFederatedQueryModifyQueryPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteQueryRequest>        GetFederatedQueryDeleteQueryPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ControlQueryRequest>       GetFederatedQueryControlQueryPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::GetResultDataRequest>      GetFederatedQueryGetResultDataPermissions();

NFederatedQuery::TPermissionsFunc<FederatedQuery::ListJobsRequest>           GetFederatedQueryListJobsPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeJobRequest>        GetFederatedQueryDescribeJobPermissions();

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateConnectionRequest>   GetFederatedQueryCreateConnectionPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ListConnectionsRequest>    GetFederatedQueryListConnectionsPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeConnectionRequest> GetFederatedQueryDescribeConnectionPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyConnectionRequest>   GetFederatedQueryModifyConnectionPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteConnectionRequest>   GetFederatedQueryDeleteConnectionPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::TestConnectionRequest>     GetFederatedQueryTestConnectionPermissions();

NFederatedQuery::TPermissionsFunc<FederatedQuery::CreateBindingRequest>      GetFederatedQueryCreateBindingPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ListBindingsRequest>       GetFederatedQueryListBindingsPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DescribeBindingRequest>    GetFederatedQueryDescribeBindingPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::ModifyBindingRequest>      GetFederatedQueryModifyBindingPermissions();
NFederatedQuery::TPermissionsFunc<FederatedQuery::DeleteBindingRequest>      GetFederatedQueryDeleteBindingPermissions();

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListQueriesRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetQueryStatusRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryControlQueryRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetResultDataRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListJobsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeJobRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListConnectionsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryTestConnectionRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListBindingsRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteBindingRequestOperationCall(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx);

} // namespace NGRpcService
} // namespace NKikimr
