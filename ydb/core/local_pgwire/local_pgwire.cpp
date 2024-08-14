#include "log_impl.h"
#include "local_pgwire.h"
#include "local_pgwire_util.h"
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/ticket_parser.h>

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

extern IActor* CreateConnection(std::unordered_map<TString, TString> params, NPG::TEvPGEvents::TEvConnectionOpened::TPtr&& event, const TConnectionState& connection);

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

    struct TConnectionState {
        TActorId YdbConnection;
        uint32_t ConnectionNum;
    };

    std::unordered_map<TActorId, TConnectionState> ConnectionState;
    std::unordered_map<TActorId, TSecurityState> SecurityState;
    std::unordered_map<TString, TTokenState> TokenState;
    uint32_t ConnectionNum = 0;

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
        BLOG_D("TEvAuth " << ev->Get()->InitialMessage->Dump() << " cookie " << ev->Cookie);
        Ydb::Auth::LoginRequest request;
        request.set_user(clientParams["user"]);
        if (ev->Get()->PasswordMessage) {
            request.set_password(TString(ev->Get()->PasswordMessage->GetPassword()));
        }
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        TActorId sender = ev->Sender;
        TString database = clientParams["database"];
        if (database == "/postgres") {
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            authResponse->Error = Ydb::StatusIds_StatusCode_Name(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_BAD_REQUEST);
            actorSystem->Send(sender, authResponse.release());
        }
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
        BLOG_D("TEvConnectionOpened " << ev->Sender << " cookie " << ev->Cookie);
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
        auto& connectionState = ConnectionState[ev->Sender];
        connectionState.ConnectionNum = ++ConnectionNum;
        IActor* actor = CreateConnection(std::move(params), std::move(ev), {.ConnectionNum = connectionState.ConnectionNum});
        TActorId actorId = Register(actor);
        connectionState.YdbConnection = actorId;
        BLOG_D("Created ydb connection " << actorId << " num " << connectionState.ConnectionNum);
    }

    void Handle(NPG::TEvPGEvents::TEvConnectionClosed::TPtr& ev) {
        BLOG_D("TEvConnectionClosed " << ev->Sender << " cookie " << ev->Cookie);
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Send(itConnection->second.YdbConnection, new TEvents::TEvPoisonPill());
            BLOG_D("Destroyed ydb connection " << itConnection->second.YdbConnection << " num " << itConnection->second.ConnectionNum);
        }
        SecurityState.erase(ev->Sender);
        ConnectionState.erase(itConnection);
        // TODO: cleanup TokenState too
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvBind::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvDescribe::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvExecute::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvClose::TPtr& ev) {
        auto itConnection = ConnectionState.find(ev->Sender);
        if (itConnection != ConnectionState.end()) {
            Forward(ev, itConnection->second.YdbConnection);
        }
    }

    void Handle(NPG::TEvPGEvents::TEvCancelRequest::TPtr& ev) {
        uint32_t nodeId = ev->Get()->Record.GetProcessId();
        if (nodeId == SelfId().NodeId()) {
            uint32_t connectionNum = ev->Get()->Record.GetSecretKey();
            for (const auto& [pgConnectionId, connectionState] : ConnectionState) {
                if (connectionState.ConnectionNum == connectionNum) {
                    BLOG_D("Cancelling ConnectionNum " << connectionNum);
                    Forward(ev, connectionState.YdbConnection);
                    return;
                }
            }
            BLOG_W("Cancelling ConnectionNum " << connectionNum << " - connection not found");
        } else {
            BLOG_D("Forwarding TEvCancelRequest to Node " << nodeId);
            Forward(ev, CreateLocalPgWireProxyId(nodeId));
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
            hFunc(NPG::TEvPGEvents::TEvClose, Handle);
            hFunc(NPG::TEvPGEvents::TEvCancelRequest, Handle);
            hFunc(TEvPrivate::TEvTokenReady, Handle);
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
        }
    }
private:

    void SendLoginRequest(NPG::TEvPGEvents::TEvAuth::TPtr& ev, 
            std::unordered_map<TString, TString>& clientParams, const NActors::TActorContext& ctx) {
        Ydb::Auth::LoginRequest request;
        request.set_user(clientParams["user"]);
        if (ev->Get()->PasswordMessage) {
            request.set_password(TString(ev->Get()->PasswordMessage->GetPassword()));
        }
        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        TActorId sender = ev->Sender;
        TString database = clientParams["database"];
        if (database == "/postgres") {
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            authResponse->Error = Ydb::StatusIds_StatusCode_Name(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_BAD_REQUEST);
            actorSystem->Send(sender, authResponse.release());
        }
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

    void SendApiKeyRequest(NPG::TEvPGEvents::TEvAuth::TPtr& ev, const std::unordered_map<TString, TString>& clientParams) {
        TString database = clientParams["database"];
        TString peerName = ev->Get()->Address;

        auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId, true);

        Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
            .Database = database,
            .Ticket = "ApiKey " + apiKey,
            .PeerName = peerName,
            .Entries = entries
        }));
    }
};


NActors::IActor* CreateLocalPgWireProxy() {
    return new TPgYdbProxy();
}

}
