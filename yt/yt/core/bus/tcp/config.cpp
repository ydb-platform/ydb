#include "config.h"

#include <yt/yt/core/net/address.h>

namespace NYT::NBus {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMultiplexingBandConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tos_level", &TThis::TosLevel)
        .Default(NYT::NBus::DefaultTosLevel);

    registrar.Parameter("network_to_tos_level", &TThis::NetworkToTosLevel)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTcpDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(8);

    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Default();

    registrar.Parameter("networks", &TThis::Networks)
        .Default();

    registrar.Parameter("multiplexing_bands", &TThis::MultiplexingBands)
        .Default();
}

TTcpDispatcherConfigPtr TTcpDispatcherConfig::ApplyDynamic(
    const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ThreadPoolSize, dynamicConfig->ThreadPoolSize);
    UpdateYsonStructField(mergedConfig->Networks, dynamicConfig->Networks);
    UpdateYsonStructField(mergedConfig->MultiplexingBands, dynamicConfig->MultiplexingBands);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TTcpDispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Optional()
        .GreaterThan(0);

    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Default();

    registrar.Parameter("networks", &TThis::Networks)
        .Default();

    registrar.Parameter("multiplexing_bands", &TThis::MultiplexingBands)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TBusServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default();
    registrar.Parameter("unix_domain_socket_path", &TThis::UnixDomainSocketPath)
        .Default();
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);
    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50000);
}

TBusServerConfigPtr TBusServerConfig::CreateTcp(int port)
{
    auto config = New<TBusServerConfig>();
    config->Port = port;
    return config;
}

TBusServerConfigPtr TBusServerConfig::CreateUds(const TString& socketPath)
{
    auto config = New<TBusServerConfig>();
    config->UnixDomainSocketPath = socketPath;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TBusConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_quick_ack", &TThis::EnableQuickAck)
        .Default(true);
    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);
    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("read_stall_timeout", &TThis::ReadStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("write_stall_timeout", &TThis::WriteStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("verify_checksums", &TThis::VerifyChecksums)
        .Default(true);
    registrar.Parameter("generate_checksums", &TThis::GenerateChecksums)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TBusClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("unix_domain_socket_path", &TThis::UnixDomainSocketPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (!config->Address && !config->UnixDomainSocketPath) {
            THROW_ERROR_EXCEPTION("\"address\" and \"unix_domain_socket_path\" cannot be both missing");
        }
    });
}

TBusClientConfigPtr TBusClientConfig::CreateTcp(const TString& address)
{
    auto config = New<TBusClientConfig>();
    config->Address = address;
    return config;
}

TBusClientConfigPtr TBusClientConfig::CreateUds(const TString& socketPath)
{
    auto config = New<TBusClientConfig>();
    config->UnixDomainSocketPath = socketPath;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
