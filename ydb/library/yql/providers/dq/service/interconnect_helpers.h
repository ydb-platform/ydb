#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/yson/node/node.h>

#include <ydb/library/yql/providers/dq/config/config.pb.h>

namespace NYql::NDqs {

enum class ENodeIdLimits {
    MinServiceNodeId = 1,
    MaxServiceNodeId = 512, // excluding
    MinWorkerNodeId = 512,
    MaxWorkerNodeId = 8192, // excluding
};

using TNameserverFactory = std::function<NActors::IActor*(const TIntrusivePtr<NActors::TTableNameserverSetup>& setup)>;

struct TServiceNodeConfig {
    ui32 NodeId;
    TString InterconnectAddress;
    TString GrpcHostname;
    ui16 Port;
    ui16 GrpcPort = 8080;
    ui16 MbusPort = 0;
    SOCKET Socket = -1;
    SOCKET GrpcSocket = -1;
    TMaybe<ui32> MaxNodeId;
    NYql::NProto::TDqConfig::TICSettings ICSettings = NYql::NProto::TDqConfig::TICSettings();
    TNameserverFactory NameserverFactory = [](const TIntrusivePtr<NActors::TTableNameserverSetup>& setup) {
        return CreateNameserverTable(setup);
    };
};

std::tuple<TString, TString> GetLocalAddress(const TString* hostname = nullptr, int family = AF_INET6);
std::tuple<TString, TString> GetUserToken(const TMaybe<TString>& user, const TMaybe<TString>& tokenFile);

std::tuple<THolder<NActors::TActorSystemSetup>, TIntrusivePtr<NActors::NLog::TSettings>> BuildActorSetup(
    ui32 nodeId,
    TString interconnectAddress,
    ui16 port,
    SOCKET socket,
    TVector<ui32> threads,
    NMonitoring::TDynamicCounterPtr counters,
    const TNameserverFactory& nameserverFactory,
    TMaybe<ui32> maxNodeId,
    const NYql::NProto::TDqConfig::TICSettings& icSettings = NYql::NProto::TDqConfig::TICSettings());

} // namespace NYql::NDqs
