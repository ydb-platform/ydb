#include "config.h"

#include "counter.h"
#include "rpc.h"

namespace NYT::NClient::NHedging::NRpc {

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("connection", &TThis::Connection);
    registrar.Parameter("initial_penalty", &TThis::InitialPenalty);
}

THedgingClientOptions::TClientOptions::TClientOptions(
    NApi::IClientPtr client,
    const std::string& clusterName,
    TDuration initialPenalty,
    TCounterPtr counter)
    : Client(std::move(client))
    , ClusterName(clusterName)
    , InitialPenalty(initialPenalty)
    , Counter(std::move(counter))
{ }

THedgingClientOptions::TClientOptions::TClientOptions(
    NApi::IClientPtr client,
    TDuration initialPenalty,
    TCounterPtr counter)
    : TClientOptions(client, "default", initialPenalty, counter)
{ }

void THedgingClientOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("client_configs", &TThis::ClientConfigs)
        .Default();
    registrar.Parameter("ban_penalty", &TThis::BanPenalty)
        .Default(TDuration::MilliSeconds(1));
    registrar.Parameter("ban_duration", &TThis::BanDuration)
        .Default(TDuration::MilliSeconds(50));
    registrar.Parameter("tags", &TThis::Tags)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        NProfiling::TTagSet counterTagSet;

        for (const auto& [tagName, tagValue] : config->Tags) {
            counterTagSet.AddTag(NProfiling::TTag(tagName, tagValue));
        }

        config->Clients.reserve(config->Clients.size());
        for (const auto& client : config->ClientConfigs) {
            THROW_ERROR_EXCEPTION_UNLESS(client->Connection->ClusterUrl, "\"cluster_url\" must be set");
            auto clusterUrl = client->Connection->ClusterUrl.value();
            config->Clients.emplace_back(
                CreateClient(client->Connection),
                clusterUrl,
                client->InitialPenalty,
                New<TCounter>(counterTagSet.WithTag(NProfiling::TTag("yt_cluster", clusterUrl))));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
