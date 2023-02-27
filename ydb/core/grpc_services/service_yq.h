#pragma once

#include <algorithm>
#include <memory>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/yq/libs/control_plane_proxy/utils.h>

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

template <typename TReq, typename TResp>
class TGrpcYqRequestOperationCall : public TGrpcRequestOperationCall<TReq, TResp> {
public:
    using TBase = TGrpcRequestOperationCall<TReq, TResp>;
    using TBase::GetProtoRequest;
    using TBase::GetPeerMetaValues;
    using NPerms = NKikimr::TEvTicketParser::TEvAuthorizeTicket;

    const std::function<TVector<NPerms::TPermission>(const TReq&)>& Permissions;
    TVector<TString> Sids;

    TGrpcYqRequestOperationCall(NGrpc::IRequestContextBase* ctx,
        void (*cb)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider& f),
        const std::function<TVector<NPerms::TPermission>(const TReq&)>& permissions)
        : TGrpcRequestOperationCall<TReq, TResp>(ctx, cb, {}), Permissions(permissions) {
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& , ICheckerIface* iface) override {
        TString scope = GetPeerMetaValues("x-ydb-fq-project").GetOrElse("");
        if (scope.empty()) {
            scope = GetPeerMetaValues("x-yq-scope").GetOrElse(""); // TODO: remove YQ-1055
        }
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

void DoYandexQueryCreateQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryListQueriesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDescribeQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryGetQueryStatusRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryModifyQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDeleteQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryControlQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoYandexQueryDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateListQueriesRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateGetQueryStatusRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateControlQueryRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateGetResultDataRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateListJobsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeJobRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateListConnectionsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateTestConnectionRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

std::unique_ptr<TEvProxyRuntimeEvent> CreateCreateBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateListBindingsRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDescribeBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateModifyBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);
std::unique_ptr<TEvProxyRuntimeEvent> CreateDeleteBindingRequestOperationCall(TIntrusivePtr<NGrpc::IRequestContextBase> ctx);

} // namespace NGRpcService
} // namespace NKikimr
