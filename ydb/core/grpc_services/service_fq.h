#pragma once

#include <algorithm>
#include <memory>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/yq/libs/control_plane_proxy/utils.h>

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

    TGrpcFqRequestOperationCall(NGrpc::IRequestContextBase* ctx,
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

                auto serviceAccountId = NYq::ExtractServiceAccountId(*GetProtoRequest());
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

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListQueriesRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetQueryStatusRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryControlQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryGetResultDataRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListJobsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeJobRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListConnectionsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryTestConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryCreateBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryListBindingsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDescribeBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryModifyBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateFederatedQueryDeleteBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

} // namespace NGRpcService
} // namespace NKikimr
