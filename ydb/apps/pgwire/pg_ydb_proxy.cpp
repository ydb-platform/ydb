#include "pg_ydb_proxy.h"
#include "pg_ydb_connection.h"
#include "log_impl.h"
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/library/actors/core/actor.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>

namespace NPGW {

using namespace NActors;

class TPgYdbProxy : public TActor<TPgYdbProxy> {
    struct TSecurityState {
        TString Token;
    };

    using TBase = TActor<TPgYdbProxy>;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::TGRpcConnectionsImpl> Connections;
    std::unordered_map<TActorId, TActorId> PgToYdbConnection;
    std::unordered_map<TActorId, TSecurityState> SecurityState;

public:
    TPgYdbProxy(const NYdb::TDriverConfig& driverConfig)
        : TBase(&TPgYdbProxy::StateWork)
        , Driver(driverConfig)
        , Connections(CreateInternalInterface(Driver))
    {
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

        auto responseCb = [this, actorSystem, sender](Ydb::Auth::LoginResponse* resp, NYdb::TPlainStatus status) {
            auto authResponse = std::make_unique<NPG::TEvPGEvents::TEvAuthResponse>();
            if (status.Ok() && resp->operation().status() == Ydb::StatusIds::SUCCESS) {
                Ydb::Auth::LoginResult result;
                resp->operation().result().UnpackTo(&result);
                this->SecurityState[sender].Token = result.token();
            } else {
                if (!status.Ok()) {
                    authResponse->Error = status.Issues.ToOneLineString();
                }
                if (resp != nullptr) {
                    if (resp->operation().issues_size() > 0) {
                        authResponse->Error = resp->operation().issues(0).message();
                    } else {
                        authResponse->Error = Ydb::StatusIds_StatusCode_Name(resp->operation().status());
                    }
                } else {
                    if (status.Status == NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED) {
                        authResponse->Error = "The database doesn't support authentication";
                    } else {
                        authResponse->Error = "Authentication error";
                    }
                }
            }
            actorSystem->Send(sender, authResponse.release());
        };

        TString database = clientParams["database"];
        NYdb::TDbDriverStatePtr driverState = Connections->GetDriverState(database, {}, {}, {}, {});

        NYdb::TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = TDuration::Seconds(60);

        NYdb::TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Auth::V1::AuthService, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>(
            driverState,
            std::move(request),
            std::move(responseCb),
            &Ydb::Auth::V1::AuthService::Stub::AsyncLogin,
            rpcSettings);
    }

    void Handle(NPG::TEvPGEvents::TEvConnectionOpened::TPtr& ev) {
        BLOG_D("TEvConnectionOpened " << ev->Sender);
        auto params = ev->Get()->Message->GetClientParams();
        auto itSecurityState = SecurityState.find(ev->Sender);
        if (itSecurityState != SecurityState.end() && !itSecurityState->second.Token.empty()) {
            params["auth-token"] = itSecurityState->second.Token;
        }
        IActor* actor = CreateConnection(Driver, std::move(params));
        TActorId actorId = Register(actor);
        PgToYdbConnection[ev->Sender] = actorId;
        BLOG_D("Created ydb connection " << actorId);
        Send(ev->Sender, new NPG::TEvPGEvents::TEvFinishHandshake(), 0, ev->Cookie);
    }

    void Handle(NPG::TEvPGEvents::TEvConnectionClosed::TPtr& ev) {
        BLOG_D("TEvConnectionClosed " << ev->Sender);
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Send(itConnection->second, new TEvents::TEvPoisonPill());
            BLOG_D("Destroyed ydb connection " << itConnection->second);
        }
        SecurityState.erase(ev->Sender);
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

    void Handle(NPG::TEvPGEvents::TEvClose::TPtr& ev) {
        auto itConnection = PgToYdbConnection.find(ev->Sender);
        if (itConnection != PgToYdbConnection.end()) {
            Forward(ev, itConnection->second);
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
        }
    }
};


NActors::IActor* CreateDatabaseProxy(const NYdb::TDriverConfig& driverConfig) {
    return new TPgYdbProxy(driverConfig);
}

}
