#pragma once

#include "public.h"

#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingBandConfig
    : public NYTree::TYsonStruct
{
public:
    int TosLevel;
    THashMap<std::string, int> NetworkToTosLevel;

    int MinMultiplexingParallelism;
    int MaxMultiplexingParallelism;

    REGISTER_YSON_STRUCT(TMultiplexingBandConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingBandConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadPoolSize;

    TDuration ThreadPoolPollingPeriod;

    //! Used for profiling export and alerts.
    std::optional<i64> NetworkBandwidth;

    THashMap<std::string, std::vector<NNet::TIP6Network>> Networks;

    TEnumIndexedArray<EMultiplexingBand, TMultiplexingBandConfigPtr> MultiplexingBands;

    //! Used to store TLS/SSL certificate files.
    std::optional<TString> BusCertsDirectoryPath;

    bool EnableLocalBypass;

    TTcpDispatcherConfigPtr ApplyDynamic(const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TTcpDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<int> ThreadPoolSize;

    std::optional<TDuration> ThreadPoolPollingPeriod;

    std::optional<i64> NetworkBandwidth;

    std::optional<THashMap<std::string, std::vector<NNet::TIP6Network>>> Networks;

    std::optional<TEnumIndexedArray<EMultiplexingBand, TMultiplexingBandConfigPtr>> MultiplexingBands;

    //! Used to store TLS/SSL certificate files.
    std::optional<TString> BusCertsDirectoryPath;

    std::optional<bool> EnableLocalBypass;

    REGISTER_YSON_STRUCT(TTcpDispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusConfig
    : public NNet::TDialerConfig
{
public:
    bool EnableQuickAck;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    TDuration ReadStallTimeout;
    TDuration WriteStallTimeout;

    std::optional<TDuration> ConnectionStartDelay;
    std::optional<TDuration> PacketDecoderDelay;

    bool VerifyChecksums;
    bool GenerateChecksums;

    bool EnableLocalBypass;

    // Ssl options.
    EEncryptionMode EncryptionMode;
    EVerificationMode VerificationMode;
    NCrypto::TPemBlobConfigPtr CA;
    NCrypto::TPemBlobConfigPtr CertificateChain;
    NCrypto::TPemBlobConfigPtr PrivateKey;
    std::optional<TString> CipherList;
    bool LoadCertsFromBusCertsDirectory;
    std::optional<TString> PeerAlternativeHostName;

    REGISTER_YSON_STRUCT(TBusConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool NeedRejectConnectionDueMemoryOvercommit;

    REGISTER_YSON_STRUCT(TBusDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusServerConfig
    : public TBusConfig
{
public:
    std::optional<int> Port;
    std::optional<std::string> UnixDomainSocketPath;
    int MaxBacklogSize;
    int MaxSimultaneousConnections;

    static TBusServerConfigPtr CreateTcp(int port);
    static TBusServerConfigPtr CreateUds(const std::string& socketPath);

    REGISTER_YSON_STRUCT(TBusServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusServerDynamicConfig
    : public TBusDynamicConfig
{
public:
    REGISTER_YSON_STRUCT(TBusServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusClientConfig
    : public TBusConfig
{
public:
    std::optional<std::string> Address;
    std::optional<std::string> UnixDomainSocketPath;

    static TBusClientConfigPtr CreateTcp(const std::string& address);
    static TBusClientConfigPtr CreateUds(const std::string& socketPath);

    REGISTER_YSON_STRUCT(TBusClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TBusClientDynamicConfig
    : public TBusDynamicConfig
{
public:
    REGISTER_YSON_STRUCT(TBusClientDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBusClientDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

