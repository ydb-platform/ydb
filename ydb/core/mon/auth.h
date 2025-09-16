#pragma once

#include "mon.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/services/services.pb.h>

#include <unordered_map>

namespace NActors {
struct THandlerInfo {
    TActorId Handler;
    TVector<TString> AllowedSIDs;
    TMon::TRequestAuthorizer Authorizer;
};

class THttpMonAuth : public TActor<THttpMonAuth> {
public:
    THttpMonAuth(const TActorId& httpProxyActorId, TMon::TRequestAuthorizer authorizer)
        : TActor(&THttpMonAuth::StateWork)
        , HttpProxyActorId(httpProxyActorId)
        , Authorizer(std::move(authorizer))
    {}

    constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::HTTP_MON_INDEX_SERVICE;
    }

private:

    THandlerInfo* FindHandler(const TString& originalPath, TString& matchedPath);

    void Handle(NHttp::TEvHttpProxy::TEvRegisterHandler::TPtr& ev);
    void Handle(TEvMon::TEvRegisterHandler::TPtr& ev);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);

    STATEFN(StateWork);

private:
    TActorId HttpProxyActorId;
    std::unordered_map<TString, THandlerInfo> Handlers;
    TMon::TRequestAuthorizer Authorizer;
};

} // namespace NActors
