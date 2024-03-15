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

    registrar.Parameter("bus_certs_directory_path", &TThis::BusCertsDirectoryPath)
        .Default();
}

TTcpDispatcherConfigPtr TTcpDispatcherConfig::ApplyDynamic(
    const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ThreadPoolSize, dynamicConfig->ThreadPoolSize);
    UpdateYsonStructField(mergedConfig->Networks, dynamicConfig->Networks);
    UpdateYsonStructField(mergedConfig->MultiplexingBands, dynamicConfig->MultiplexingBands);
    UpdateYsonStructField(mergedConfig->BusCertsDirectoryPath, dynamicConfig->BusCertsDirectoryPath);
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

    registrar.Parameter("bus_certs_directory_path", &TThis::BusCertsDirectoryPath)
        .Default();
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
    registrar.Parameter("connection_start_delay", &TThis::ConnectionStartDelay)
        .Default();
    registrar.Parameter("packet_decoder_delay", &TThis::PacketDecoderDelay)
        .Default();
    registrar.Parameter("read_stall_timeout", &TThis::ReadStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("write_stall_timeout", &TThis::WriteStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("verify_checksums", &TThis::VerifyChecksums)
        .Default(true);
    registrar.Parameter("generate_checksums", &TThis::GenerateChecksums)
        .Default(true);
    registrar.Parameter("encryption_mode", &TThis::EncryptionMode)
        .Default(EEncryptionMode::Optional);
    registrar.Parameter("verification_mode", &TThis::VerificationMode)
        .Default(EVerificationMode::None);
    registrar.Parameter("ca", &TThis::CA)
        .Default();
    registrar.Parameter("cert_chain", &TThis::CertificateChain)
        .Default();
    registrar.Parameter("private_key", &TThis::PrivateKey)
        .Default();
    registrar.Parameter("cipher_list", &TThis::CipherList)
        .Default();
    registrar.Parameter("load_certs_from_bus_certs_directory", &TThis::LoadCertsFromBusCertsDirectory)
        .Default(false);
    registrar.Parameter("peer_alternative_host_name", &TThis::PeerAlternativeHostName)
        .Optional();
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
