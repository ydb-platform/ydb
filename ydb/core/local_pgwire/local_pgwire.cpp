#include "log_impl.h"
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <library/cpp/actors/core/actor.h>
#include <ydb/core/base/ticket_parser.h>

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

extern IActor* CreateConnection(std::unordered_map<TString, TString> params);

class TPgYdbProxy : public TActor<TPgYdbProxy> {
    using TBase = TActor<TPgYdbProxy>;

    struct TSecurityState {
        TString Ticket;
        Ydb::Auth::LoginResult LoginResult;
        TEvTicketParser::TError Error;
        TIntrusiveConstPtr<NACLib::TUserToken> Token;
        TString SerializedToken;
    };

    struct TTokenState {
        std::unordered_set<TActorId> Senders;
    };

    struct TEvPrivate {
        enum EEv {
            EvTokenReady = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvTokenReady : TEventLocal<TEvTokenReady, EvTokenReady> {
            Ydb::Auth::LoginResult LoginResult;
            TActorId Sender;
            TString Database;
            TString PeerName;

            TEvTokenReady() = default;
        };
    };

    std::unordered_map<TActorId, TActorId> PgToYdbConnection;
    std::unordered_map<TActorId, TSecurityState> SecurityState;
    std::unordered_map<TString, TTokenState> TokenState;

public:
    TPgYdbProxy()
        : TBase(&TPgYdbProxy::StateWork)
    {
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        auto token = ev->Get()->Ticket;
        auto itTokenState = TokenState.find(token);
        if (itTokenState == TokenState.end()) {
            BLOG_W("Couldn't find token in reply from TicketParser");
            return;
        }
        for (auto sender : itTokenState->second.Senders) {
            auto& securityState(SecurityState[sender]);
            securityState.Ticket = token;
            securityState.Error = ev->Get()->Error;
            securityState.Token = ev->Get()->Token;
            securityState.SerializedToken = ev->Get()->SerializedToken;
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            if (ev->Get()->Error) {
                authResponse->Error = ev->Get()->Error.Message;
            }
            Send(sender, authResponse.release());
        }
        TokenState.erase(itTokenState);
    }

    void Handle(TEvPrivate::TEvTokenReady::TPtr& ev) {
        auto token = ev->Get()->LoginResult.token();
        auto itTokenState = TokenState.find(token);
        if (itTokenState == TokenState.end()) {
            itTokenState = TokenState.insert({token, {}}).first;
        }
        bool needSend = itTokenState->second.Senders.empty();
        itTokenState->second.Senders.insert(ev->Get()->Sender);
        if (needSend) {
            Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
                .Database = ev->Get()->Database,
                .Ticket = token,
                .PeerName = ev->Get()->PeerName,
            }));
        }
        SecurityState[ev->Get()->Sender].LoginResult = std::move(ev->Get()->LoginResult);
    }

    void Handle(NPG::TEvPGEvents::TEvAuth::TPtr& ev) {
        std::unordered_map<TString, TString> clientParams = ev->Get()->InitialMessage->GetClientParams();
        BLOG_D("TEvAuth " << ev->Get()->InitialMessage->Dump());
        Ydb::Auth::LoginRequest request;
        request.set_user(clientParams["user"]);
        if (ev->Get()->PasswordMessage) {
            request.set_password(TString(ev->Get()->PasswordMessage->GetPassword()));
        }
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        TActorId sender = ev->Sender;
        TString database = clientParams["database"];
        TString peerName = TStringBuilder() << ev->Get()->Address;

        using TRpcEv = NGRpcService::TGRpcRequestWrapperNoAuth<NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;
        auto rpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), database, {}, actorSystem);
        rpcFuture.Subscribe([actorSystem, sender, database, peerName, selfId = SelfId()](const NThreading::TFuture<Ydb::Auth::LoginResponse>& future) {
            auto& response = future.GetValueSync();
            if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                auto tokenReady = std::make_unique<TEvPrivate::TEvTokenReady>();
                response.operation().result().UnpackTo(&(tokenReady->LoginResult));
                tokenReady->Sender = sender;
                tokenReady->Database = database;
                tokenReady->PeerName = peerName;
                actorSystem->Send(selfId, tokenReady.release());
            } else {
                auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
                if (response.operation().issues_size() > 0) {
                    authResponse->Error = response.operation().issues(0).message();
                } else {
                    authResponse->Error = Ydb::StatusIds_StatusCode_Name(response.operation().status());
                }
                actorSystem->Send(sender, authResponse.release());
            }
        });
    }

    void Handle(NPG::TEvPGEvents::TEvConnectionOpened::TPtr& ev) {
        BLOG_D("TEvConnectionOpened " << ev->Sender);
        auto params = ev->Get()->Message->GetClientParams();
        auto itSecurityState = SecurityState.find(ev->Sender);
        if (itSecurityState != SecurityState.end()) {
            if (!itSecurityState->second.Ticket.empty()) {
                params["auth-token"] = itSecurityState->second.Ticket;
            }
            if (!itSecurityState->second.SerializedToken.empty()) {
                params["ydb-serialized-token"] = itSecurityState->second.SerializedToken;
            }
        }
        IActor* actor = CreateConnection(std::move(params));
        TActorId actorId = Register(actor);
        PgToYdbConnection[ev->Sender] = actorId;
        BLOG_D("Created ydb connection " << actorId);
    }

    void Handle(NPG::TEvPGEvents::TEvConnectionClosed::TPtr& ev) {
        BLOG_D("TEvConnectionClosed " << ev->Sender);
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Send(itConnection->second, new TEvents::TEvPoisonPill());
            BLOG_D("Destroyed ydb connection " << itConnection->second);
        }
        SecurityState.erase(ev->Sender);
        // TODO: cleanup TokenState too
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvBind::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvDescribe::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvExecute::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
            BLOG_D("Forwarded to ydb connection " << itConnection->second);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPG::TEvPGEvents::TEvAuth, Handle);
            hFunc(NPG::TEvPGEvents::TEvConnectionOpened, Handle);
            hFunc(NPG::TEvPGEvents::TEvConnectionClosed, Handle);
            hFunc(NPG::TEvPGEvents::TEvQuery, Handle);
            hFunc(NPG::TEvPGEvents::TEvParse, Handle);
            hFunc(NPG::TEvPGEvents::TEvBind, Handle);
            hFunc(NPG::TEvPGEvents::TEvDescribe, Handle);
            hFunc(NPG::TEvPGEvents::TEvExecute, Handle);
            hFunc(TEvPrivate::TEvTokenReady, Handle);
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
        }
    }
};


NActors::IActor* CreateLocalPgWireProxy() {
    return new TPgYdbProxy();
}

}
