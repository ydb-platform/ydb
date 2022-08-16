#pragma once

#include <library/cpp/actors/core/actor.h>
#include "pg_proxy_config.h"
#include "pg_proxy_impl.h"

namespace NPG {

struct TListenerSettings {
    uint16_t Port = 5432;
    TString CertificateFile;
    TString PrivateKeyFile;
    TString SslCertificatePem;
};

NActors::IActor* CreatePGListener(const NActors::TActorId& poller, const NActors::TActorId& databaseProxy, const TListenerSettings& settings = {});

}

