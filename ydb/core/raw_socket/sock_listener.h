#pragma once

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/protos/services.pb.h>

#include "sock_impl.h"
#include "sock_config.h"

namespace NKikimr::NRawSocket {

struct TListenerSettings {
    uint16_t Port;
    TString CertificateFile;
    TString PrivateKeyFile;
    TString SslCertificatePem;
};

using TConnectionCreator = std::function<NActors::IActor* (TIntrusivePtr<TSocketDescriptor> socket, TNetworkConfig::TSocketAddressType address)>;

NActors::IActor* CreateSocketListener(const NActors::TActorId& poller, const TListenerSettings& settings,
                                      TConnectionCreator connectionCreator, NKikimrServices::EServiceKikimr service);

} // namespace NKikimr::NRawSocket
