#pragma once

#include "grpc_endpoint.h"


#include "grpc_request_proxy_handle_methods.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <type_traits>

namespace NKikimrConfig {
class TAppConfig;
}

namespace NKikimr {

struct TAppData;

namespace NGRpcService {

IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig);
IActor* CreateGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig);
TString ResolveDatabaseName(const TString& databaseName, const TString& rootDatabase);

template <typename TEvent>
bool ResolveRequestDatabase(TEvent* request, const TString& rootDatabase, bool ignoreRoot) {
    if (request->IsInternalCall() || rootDatabase.empty()) {
        return true;
    }
    request->SetUseDatabaseRootAlias(ignoreRoot);

    const auto resolve = [&](const TString& database) {
        // Preserve invalid all-slash names for the normal empty-path validation.
        if (database.empty() || CanonizePath(database).empty()) {
            return database;
        }
        return ResolveDatabaseName(ignoreRoot ? ResolveDatabasePath(database, rootDatabase) : database, rootDatabase);
    };

    const auto database = request->GetDatabaseName();
    if constexpr (std::is_same_v<TEvent, TEvListEndpointsRequest>) {
        const auto& discoveryDatabase = request->GetProtoRequest()->database();
        if (ignoreRoot && !discoveryDatabase.empty()) {
            const TString resolved = resolve(discoveryDatabase);
            const auto header = request->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
            if (header && !header->empty() && resolve(CGIUnescapeRet(*header)) != resolved) {
                request->RaiseIssue(NYql::TIssue("ListEndpoints database and x-ydb-database resolve to different databases"));
                return false;
            }
            request->SetDatabaseName(resolved);
            return true;
        }
    }
    if (database && !database->empty()) {
        request->SetDatabaseName(resolve(*database));
    }
    return true;
}

class TGRpcRequestProxy : public TGRpcRequestProxyHandleMethods, public IFacilityProvider {
public:
    enum EEv {
        EvRefreshTokenResponse = EventSpaceBegin(TKikimrEvents::ES_GRPC_REQUEST_PROXY),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_REQUEST_PROXY),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_REQUEST_PROXY)");

    struct TEvRefreshTokenResponse : public TEventLocal<TEvRefreshTokenResponse, EvRefreshTokenResponse> {
        bool Authenticated;
        TIntrusiveConstPtr<NACLib::TUserToken> InternalToken;
        bool Retryable;
        NYql::TIssues Issues;

        TEvRefreshTokenResponse(bool ok, const TIntrusiveConstPtr<NACLib::TUserToken>& token, bool retryable, const NYql::TIssues& issues)
            : Authenticated(ok)
            , InternalToken(token)
            , Retryable(retryable)
            , Issues(issues)
        {}
    };

protected:
    using TGRpcRequestProxyHandleMethods::Handle;
    void Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx);

    TActorId DiscoveryCacheActorID;
};

} // namespace NGRpcService
} // namespace NKikimr
