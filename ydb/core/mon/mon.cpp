#include "mon.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>

namespace NActors {

using namespace NMonitoring;

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
    TStringBuf ydbSessionId = request.GetCookie("ydb_session_id");
    TStringBuf authorization = request.GetHeader("Authorization");
    if (!authorization.empty()) {
        return new NActors::IEventHandle(
            NKikimr::MakeTicketParserID(),
            owner,
            new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
                .Ticket = TString(authorization)
            }),
            IEventHandle::FlagTrackDelivery
        );
    } else if (!ydbSessionId.empty()) {
        return new NActors::IEventHandle(
            NKikimr::MakeTicketParserID(),
            owner,
            new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
                .Ticket = TString("Login ") + TString(ydbSessionId)
            }),
            IEventHandle::FlagTrackDelivery
        );
    } else if (NKikimr::AppData()->EnforceUserTokenRequirement && NKikimr::AppData()->DefaultUserSIDs.empty()) {
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::TEvTicketParser::TEvAuthorizeTicketResult(TString(), {
                .Message = "No security credentials were provided",
                .Retryable = false
            })
        );
    } else if (!NKikimr::AppData()->DefaultUserSIDs.empty()) {
        TIntrusivePtr<NACLib::TUserToken> token = new NACLib::TUserToken(NKikimr::AppData()->DefaultUserSIDs);
        return new NActors::IEventHandle(
            owner,
            owner,
            new NKikimr::TEvTicketParser::TEvAuthorizeTicketResult(TString(), token)
        );
    } else {
        return nullptr;
    }
}

}
