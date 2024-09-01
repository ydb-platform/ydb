#include "config.h"

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

void TConnectionWithPenaltyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("initial_penalty", &TThis::InitialPenalty)
        .Optional();
}

void THedgingClientOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("connections", &TThis::Connections)
        .NonEmpty();
    registrar.Parameter("ban_penalty", &TThis::BanPenalty)
        .Default(TDuration::MilliSeconds(1));
    registrar.Parameter("ban_duration", &TThis::BanDuration)
        .Default(TDuration::MilliSeconds(50));
    registrar.Parameter("tags", &TThis::Tags)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
