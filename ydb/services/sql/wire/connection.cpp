#include "connection.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NSql {

namespace {

class TSqlConnection: public NActors::TActorBootstrapped<TSqlConnection> {
public:
    TSqlConnection(const NActors::TActorId& listenerActorId,
                   TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                   NRawSocket::TNetworkConfig::TSocketAddressType address)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(std::move(address))
    {
    }

    void Bootstrap() {
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::PGWIRE,
            "sql connection accepted from " << Address << " by " << ListenerActorId);
        Become(&TSqlConnection::StateWork);
    }

private:
    void Handle(NActors::TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerRegisterResult, Handle);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ListenerActorId;
    TIntrusivePtr<NRawSocket::TSocketDescriptor> Socket;
    NRawSocket::TNetworkConfig::TSocketAddressType Address;
    NActors::TPollerToken::TPtr PollerToken;
};

} // namespace

NActors::IActor* CreateSqlConnection(const NActors::TActorId& listenerActorId,
                                     TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                                     NRawSocket::TNetworkConfig::TSocketAddressType address) {
    return new TSqlConnection(listenerActorId, std::move(socket), std::move(address));
}

} // namespace NKikimr::NSql
