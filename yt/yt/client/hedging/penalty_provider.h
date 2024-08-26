#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/profiling/public.h>

// @brief    IPenaltyProvider interface is used in HedgingClient to provide external penalties for different clusters.
//           Current implementations are DummyPenaltyProvider and ReplicationLagPenaltyProvider.
namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IPenaltyProvider
    : public TRefCounted
{
    virtual NProfiling::TCpuDuration Get(const std::string& cluster) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPenaltyProvider)

////////////////////////////////////////////////////////////////////////////////

// @brief DummyPenaltyProvider - always returns 0.
IPenaltyProviderPtr CreateDummyPenaltyProvider();

// From config.proto.
class TReplicationLagPenaltyProviderConfig;

// @brief ReplicationLagPenaltyProvider - periodically checks replication lag for given table AND replica cluster.
//        Based on values from TReplicationLagPenaltyProviderConfig add current number of tablets with lag, it either returns 0 or LagPenalty value.
//        Master client - main cluster with replicated table. ReplicaCluster + TablePath specifies concrete replica for table from main cluster.
IPenaltyProviderPtr CreateReplicationLagPenaltyProvider(
    TReplicationLagPenaltyProviderConfig config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
