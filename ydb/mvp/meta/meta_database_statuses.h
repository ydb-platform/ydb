#pragma once

#include "meta_health.h"

namespace NMVP {

NActors::IActor* CreateDatabaseStatusesHandler(const NActors::TActorId& httpProxy,
                                              const NActors::TActorId& queryService,
                                              const TYdbLocation& location,
                                              TMetaSettings::THealthcheckSettings settings,
                                              TString serviceEndpoint = {});

NActors::IActor* CreateCachedHealthcheckHandler(const NActors::TActorId& httpProxy,
                                               const NActors::TActorId& queryService,
                                               const NActors::TActorId& monitor,
                                               const TYdbLocation& location,
                                               const TMetaSettings& settings, TString localEndpoint);

} // namespace NMVP
