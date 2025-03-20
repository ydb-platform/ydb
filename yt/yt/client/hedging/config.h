#pragma once

#include "public.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/vector.h>

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionWithPenaltyConfig
    : public virtual NApi::NRpcProxy::TConnectionConfig
{
    TDuration InitialPenalty;

    REGISTER_YSON_STRUCT(TConnectionWithPenaltyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConnectionWithPenaltyConfig)

////////////////////////////////////////////////////////////////////////////////

//! The options for hedging client.
//! TODO(bulatman) Rename to `THedgingClientConfig`.
struct THedgingClientOptions
    : public virtual NYTree::TYsonStruct
{
    std::vector<TIntrusivePtr<TConnectionWithPenaltyConfig>> Connections;
    TDuration BanPenalty;
    TDuration BanDuration;
    THashMap<TString, TString> Tags;

    REGISTER_YSON_STRUCT(THedgingClientOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THedgingClientOptions)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationLagPenaltyProviderOptions
    : public virtual NYTree::TYsonStruct
{
    // Clusters that need checks for replication lag.
    std::vector<std::string> ReplicaClusters;

    // Table that needs checks for replication lag.
    NYPath::TYPath TablePath;

    // Same as BanPenalty in hedging client.
    TDuration LagPenalty;
    // Tablet is considered "lagged" if CurrentTimestamp - TabletLastReplicationTimestamp >= MaxTabletLag.
    TDuration MaxTabletLag;

    // Real value from 0.0 to 1.0. Replica cluster receives LagPenalty if NumberOfTabletsWithLag >= MaxTabletsWithLagFraction * TotalNumberOfTablets.
    double MaxTabletsWithLagFraction;

    // Replication lag check period.
    TDuration CheckPeriod;

    // In case of any errors from master client - clear all penalties.
    bool ClearPenaltiesOnErrors;

    REGISTER_YSON_STRUCT(TReplicationLagPenaltyProviderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationLagPenaltyProviderOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
