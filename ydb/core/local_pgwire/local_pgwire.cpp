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
        TString SerializedToken;
        TString Ticket;
    };

    struct TConnectionState {
        TActorId YdbConnection;
        uint32_t ConnectionNum;
    };

    std::unordered_map<TActorId, TConnectionState> ConnectionState;
    std::unordered_map<TActorId, TSecurityState> SecurityState;
    uint32_t ConnectionNum = 0;

public:
    TPgYdbProxy()
        : TBase(&TPgYdbProxy::StateWork)
    {
    }

    void Handle(NPG::TEvPGEvents::TEvAuth::TPtr& ev) {
        BLOG_D("TEvAuth " << ev->Get()->InitialMessage->Dump() << " cookie " << ev->Cookie);
        std::unordered_map<TString, TString> clientParams = ev->Get()->InitialMessage->GetClientParams();
        TPgWireAuthData pgWireAuthData;
        pgWireAuthData.UserName = clientParams["user"];
        if (ev->Get()->PasswordMessage) {
            pgWireAuthData.Password = TString(ev->Get()->PasswordMessage->GetPassword());
        }
        pgWireAuthData.Sender = ev->Sender;
        pgWireAuthData.DatabasePath = clientParams["database"];
        if (pgWireAuthData.DatabasePath == "/postgres") {
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            authResponse->Error = Ydb::StatusIds_StatusCode_Name(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_BAD_REQUEST);
            Send(pgWireAuthData.Sender, authResponse.release());
        }
        pgWireAuthData.PeerName = TStringBuilder() << ev->Get()->Address;

        Register(CreateLocalPgWireAuthActor(pgWireAuthData, SelfId()));
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

    void Handle(TEvEvents::TEvAuthResponse::TPtr& ev) {
        auto& securityState = SecurityState[ev->Get()->Sender];
        if (!ev->Get()->ErrorMessage.empty()) {
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            authResponse->Error = ev->Get()->ErrorMessage;
            Send(ev->Get()->Sender, authResponse.release());
            return;
        }
        
        securityState.SerializedToken = ev->Get()->SerializedToken;
        securityState.Ticket = ev->Get()->Ticket;

        auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
        Send(ev->Get()->Sender, authResponse.release());
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
            hFunc(TEvEvents::TEvAuthResponse, Handle);
        }
    }
};


NActors::IActor* CreateLocalPgWireProxy() {
    return new TPgYdbProxy();
}

}
