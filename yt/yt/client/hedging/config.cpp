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

void TReplicationLagPenaltyProviderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_clusters", &TThis::ReplicaClusters)
        .NonEmpty();
    registrar.Parameter("table_path", &TThis::TablePath);
    registrar.Parameter("lag_penalty", &TThis::LagPenalty)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("max_replica_lag", &TThis::MaxReplicaLag)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("check_period", &TThis::CheckPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("clear_penalties_on_errors", &TThis::ClearPenaltiesOnErrors)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
