#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>

#include "sock_impl.h"
#include "sock_config.h"

namespace NKikimr::NRawSocket {

struct TListenerSettings {
    uint16_t Port;
    TString CertificateFile;
    TString PrivateKeyFile;
    TString SslCertificatePem;
    TString Address = "::";
    bool TcpNotDelay = false;
};

enum EErrorAction {
    Ignore,
    Abort
};

using TConnectionCreator = std::function<NActors::IActor* (const TActorId& listenerActorId,
                                                           TIntrusivePtr<TSocketDescriptor> socket,
                                                           TNetworkConfig::TSocketAddressType address)>;

NActors::IActor* CreateSocketListener(const NActors::TActorId& poller, const TListenerSettings& settings,
                                      TConnectionCreator connectionCreator, NKikimrServices::EServiceKikimr service,
                                      EErrorAction errorAction = EErrorAction::Ignore);

} // namespace NKikimr::NRawSocket
