#pragma once

#include <algorithm>
#include <memory>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/utils.h>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

template <typename TReq, typename TResp>
class TGrpcFqRequestOperationCall : public TGrpcRequestOperationCall<TReq, TResp> {
public:
    using TBase = TGrpcRequestOperationCall<TReq, TResp>;
    using TBase::GetProtoRequest;
    using TBase::GetPeerMetaValues;
    using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

    const std::function<TVector<NPerms::TPermission>(const TReq&)>& Permissions;
    TVector<TString> Sids;

    TGrpcFqRequestOperationCall(NYdbGrpc::IRequestContextBase* ctx,
        void (*cb)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider& f),
        const std::function<TVector<NPerms::TPermission>(const TReq&)>& permissions)
        : TGrpcRequestOperationCall<TReq, TResp>(ctx, cb, {}), Permissions(permissions) {
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& , ICheckerIface* iface) override {
        TString scope = GetPeerMetaValues("x-ydb-fq-project").GetOrElse("");
        if (scope.StartsWith("yandexcloud://")) {
            const TVector<TString> path = StringSplitter(scope).Split('/').SkipEmpty();
            if (path.size() == 2 || path.size() == 3) {
                const TString& folderId = path.back();
                const auto& permissions = Permissions(*GetProtoRequest());
                TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> entries {{
                    permissions,
                    {
                        {"folder_id", folderId}
                    }
                }};
                std::transform(permissions.begin(), permissions.end(), std::back_inserter(Sids),
                   [](const auto& s) -> TString { return s.Permission + "@as"; });

                auto serviceAccountId = NFq::ExtractServiceAccountId(*GetProtoRequest());
                if (serviceAccountId) {
                    entries.push_back({
                        {{NPerms::Required("iam.serviceAccounts.use")}},
                        {
                            {"service_account_id", serviceAccountId}
                        }});
                    Sids.push_back("iam.serviceAccounts.use@as");
                }

                iface->SetEntries(entries);
                return true;
            }
        }

        return false;
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
