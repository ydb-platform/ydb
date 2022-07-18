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
        void (*cb)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&),
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
void DoGetResultDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoListJobsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoDescribeJobRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoCreateConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoListConnectionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoDescribeConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoModifyConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoDeleteConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoTestConnectionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoCreateBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoListBindingsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoDescribeBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoModifyBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);
void DoDeleteBindingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility);

} // namespace NGRpcService
} // namespace NKikimr
