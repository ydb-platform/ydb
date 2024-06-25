#include "mon.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/protos/auth.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>

namespace NActors {

using namespace NMonitoring;
using namespace NKikimr;

namespace {

/*
const std::vector<NKikimr::TEvTicketParser::TEvAuthorizeTicket::TEntry>& GetEntries(const TString& ticket) {
    if (ticket.StartsWith("Bearer")) {
        if (AppData()->AuthConfig.GetUseAccessService()
            && (AppData()->DomainsConfig.GetSecurityConfig().ViewerAllowedSIDsSize() > 0 || AppData()->DomainsConfig.GetSecurityConfig().MonitoringAllowedSIDsSize() > 0)) {
            static std::vector<NKikimr::TEvTicketParser::TEvAuthorizeTicket::TEntry> entries = {
                {NKikimr::TEvTicketParser::TEvAuthorizeTicket::ToPermissions({"ydb.developerApi.get", "ydb.developerApi.update"}), {{"gizmo_id", "gizmo"}}}
            };
            return entries;
        }
    }
    static std::vector<NKikimr::TEvTicketParser::TEvAuthorizeTicket::TEntry> emptyEntries = {};
    return emptyEntries;
}
*/

TString GetDatabase(NMonitoring::IMonHttpRequest& request) {
    if (const auto dbIt = request.GetParams().Find("database"); dbIt != request.GetParams().end()) {
        return dbIt->second;
    }
    if (request.GetMethod() == HTTP_METHOD_POST) {
        static NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue requestData;
        if (NJson::ReadJsonTree(request.GetPostContent(), &JsonConfig, &requestData)) {
            return requestData["database"].GetString();
        }
    }
    return {};
}

IEventHandle* GetRequestAuthAndCheckHandle(const NActors::TActorId& owner, const TString& database, const TString& ticket) {
    return new NActors::IEventHandle(
        NGRpcService::CreateGRpcRequestProxyId(),
        owner,
        new NKikimr::NGRpcService::TEvRequestAuthAndCheck(
            database,
            ticket ? TMaybe<TString>(ticket) : Nothing()),
        IEventHandle::FlagTrackDelivery
    );
}

} // namespace

NActors::IEventHandle* SelectAuthorizationScheme(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request) {
    TStringBuf ydbSessionId = request.GetCookie("ydb_session_id");
    TStringBuf authorization = request.GetHeader("Authorization");
    if (!authorization.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString(authorization));
    } else if (!ydbSessionId.empty()) {
        return GetRequestAuthAndCheckHandle(owner, GetDatabase(request), TString("Login ") + TString(ydbSessionId));
    } else {
        return nullptr;
    }
}

NActors::IEventHandle* GetAuthorizeTicketResult(const NActors::TActorId& owner) {
    if (NKikimr::AppData()->EnforceUserTokenRequirement && NKikimr::AppData()->DefaultUserSIDs.empty()) {
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::NGRpcService::TEvRequestAuthAndCheckResult(
                Ydb::StatusIds::UNAUTHORIZED,
                "No security credentials were provided")
        );
    } else if (!NKikimr::AppData()->DefaultUserSIDs.empty()) {
        TIntrusivePtr<NACLib::TUserToken> token = new NACLib::TUserToken(NKikimr::AppData()->DefaultUserSIDs);
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::NGRpcService::TEvRequestAuthAndCheckResult(
                {},
                {},
                token
            )
        );
    } else {
        return nullptr;
    }
}

IMonPage* TMon::RegisterActorPage(TIndexMonPage* index, const TString& relPath,
    const TString& title, bool preTag, TActorSystem* actorSystem, const TActorId& actorId, bool useAuth, bool sortPages) {
    return RegisterActorPage({
        .Title = title,
        .RelPath = relPath,
        .ActorSystem = actorSystem,
        .Index = index,
        .PreTag = preTag,
        .ActorId = actorId,
        .UseAuth = useAuth,
        .SortPages = sortPages,
    });
}

NActors::IEventHandle* TMon::DefaultAuthorizer(const NActors::TActorId& owner, NMonitoring::IMonHttpRequest& request) {
    NActors::IEventHandle* eventHandle = SelectAuthorizationScheme(owner, request);
    if (eventHandle != nullptr) {
        return eventHandle;
    }
    return GetAuthorizeTicketResult(owner);
}

}
