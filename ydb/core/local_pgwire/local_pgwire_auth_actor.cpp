#include "log_impl.h"
#include "local_pgwire.h"
#include "local_pgwire_util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

class TPgYdbAuthActor : public NActors::TActorBootstrapped<TPgYdbAuthActor> {
    using TBase = TActor<TPgYdbAuthActor>;

    struct TEvPrivate {
        enum EEv {
            EvTokenReady = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvAuthFailed,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvTokenReady : TEventLocal<TEvTokenReady, EvTokenReady> {
            Ydb::Auth::LoginResult LoginResult;

            TEvTokenReady() = default;
        };

        struct TEvAuthFailed : NActors::TEventLocal<TEvAuthFailed, EvAuthFailed> {
            TString ErrorMessage;
        };
    };

    TPgWireAuthData PgWireAuthData;
    TActorId PgYdbProxy; 

    TString DatabaseId;
    TString FolderId;
    TString SerializedToken;
    TString Ticket;

public:
    TPgYdbAuthActor(const TPgWireAuthData& pgWireAuthData, const TActorId& pgYdbProxy)
        : PgWireAuthData(pgWireAuthData)
        , PgYdbProxy(pgYdbProxy) {
    }

    void Bootstrap() {
        if (PgWireAuthData.UserName == "__ydb_apikey") {
            if (PgWireAuthData.Password.empty()) {
                SendResponseAndDie("Invalid password");
            }
            SendDescribeRequest();
        } else {
            SendLoginRequest();
        }

        Become(&TPgYdbAuthActor::StateWork);
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        if (ev->Get()->Error) {
            SendResponseAndDie(ev->Get()->Error.Message);
            return;
        }

        SerializedToken = ev->Get()->SerializedToken;
        Ticket = ev->Get()->Ticket;

        SendResponseAndDie();
    }

    void Handle(TEvPrivate::TEvTokenReady::TPtr& ev) {
        Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
            .Database = PgWireAuthData.DatabasePath,
            .Ticket = ev->Get()->LoginResult.token(),
            .PeerName = PgWireAuthData.PeerName,
        }));
    }

    void Handle(TEvPrivate::TEvAuthFailed::TPtr& ev) {
        SendResponseAndDie(ev->Get()->ErrorMessage);
    }

    void Handle(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NKikimr::NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        if (navigate->ErrorCount) {
            SendResponseAndDie(TStringBuilder() << "Database with path '" << PgWireAuthData.DatabasePath << "' doesn't exists");
            return;
        }
        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);

        const auto& entry = navigate->ResultSet.front();

        for (const auto& attr : entry.Attributes) {
            if (attr.first == "folderId") FolderId = attr.second;
            else if (attr.first == "database_id") DatabaseId = attr.second;
        }

        SendApiKeyRequest();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvTokenReady, Handle);
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvPrivate::TEvAuthFailed, Handle);
        }
    }
private:
    void SendLoginRequest() {
        Ydb::Auth::LoginRequest request;
        request.set_user(PgWireAuthData.UserName);
        if (!PgWireAuthData.Password.empty()) {
            request.set_password(PgWireAuthData.Password);
        }

        auto* actorSystem = TActivationContext::ActorSystem();;

        using TRpcEv = NGRpcService::TGRpcRequestWrapperNoAuth<NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;
        auto rpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), PgWireAuthData.DatabasePath, {}, actorSystem);
        rpcFuture.Subscribe([actorSystem, selfId = SelfId()](const NThreading::TFuture<Ydb::Auth::LoginResponse>& future) {
            auto& response = future.GetValueSync();
            if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                auto tokenReady = std::make_unique<TEvPrivate::TEvTokenReady>();
                response.operation().result().UnpackTo(&(tokenReady->LoginResult));
                actorSystem->Send(selfId, tokenReady.release());
            } else {
                auto authFailedEvent = std::make_unique<TEvPrivate::TEvAuthFailed>();
                if (response.operation().issues_size() > 0) {
                    authFailedEvent->ErrorMessage = response.operation().issues(0).message();
                } else {
                    authFailedEvent->ErrorMessage = Ydb::StatusIds_StatusCode_Name(response.operation().status());
                }
                actorSystem->Send(selfId, authFailedEvent.release());
            }
        });
    }

    void SendApiKeyRequest() {
        auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId);

        Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
            .Database = PgWireAuthData.DatabasePath,
            .Ticket = "ApiKey " + PgWireAuthData.Password,
            .PeerName = PgWireAuthData.PeerName,
            .Entries = entries
        }));
    }

    void SendDescribeRequest() {
        auto schemeCacheRequest = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
        NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(PgWireAuthData.DatabasePath);
        entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.SyncVersion = false;
        schemeCacheRequest->ResultSet.emplace_back(entry);
        Send(NKikimr::MakeSchemeCacheID(), MakeHolder<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
    }

    void SendResponseAndDie(const TString& errorMessage = "") {
        std::unique_ptr<TEvEvents::TEvAuthResponse> authResponse;
        if (!errorMessage.empty()) {
            authResponse = std::make_unique<TEvEvents::TEvAuthResponse>(errorMessage, PgWireAuthData.Sender);
        } else {
            authResponse = std::make_unique<TEvEvents::TEvAuthResponse>(SerializedToken, Ticket, PgWireAuthData.Sender);
        }

        Send(PgYdbProxy, authResponse.release());

        PassAway();
    }
};


NActors::IActor* CreateLocalPgWireAuthActor(const TPgWireAuthData& pgWireAuthData, const TActorId& pgYdbProxy) {
    return new TPgYdbAuthActor(pgWireAuthData, pgYdbProxy);
}

}
