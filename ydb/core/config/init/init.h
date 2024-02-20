#pragma once

#include <ydb/core/base/event_filter.h>
#include <ydb/core/cms/console/config_item_info.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

#include <memory>

namespace NKikimr::NConfig {

class IEnv {
public:
    virtual ~IEnv() {}
    virtual TString HostName() const = 0;
    virtual TString FQDNHostName() const = 0;
    virtual TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty = true) const = 0;
    virtual void Sleep(const TDuration& dur) const = 0;
};

class IErrorCollector {
public:
    virtual ~IErrorCollector() {}
    virtual void Fatal(TString error) = 0;
};

class IProtoConfigFileProvider {
public:
    virtual ~IProtoConfigFileProvider() {}
    virtual void AddConfigFile(TString optName, TString description) = 0;
    virtual void RegisterCliOptions(NLastGetopt::TOpts& opts) const = 0;
    virtual TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const = 0;
    virtual bool Has(TString optName) = 0;
    virtual TString Get(TString optName) = 0;
};

class IConfigUpdateTracer {
public:
    virtual ~IConfigUpdateTracer() {}
    virtual void Add(ui32 kind, TConfigItemInfo::TUpdate) = 0;
    virtual THashMap<ui32, TConfigItemInfo> Dump() const = 0;
};

// ===

class IMemLogInitializer {
public:
    virtual ~IMemLogInitializer() {}
    virtual void Init(const NKikimrConfig::TMemoryLogConfig& mem) const = 0;
};

// ===

struct TGrpcSslSettings {
    TString PathToGrpcCertFile;
    TString PathToGrpcCaFile;
    TString PathToGrpcPrivateKeyFile;
};

struct TNodeRegistrationSettings {
    TString DomainName;
    TString NodeHost;
    TString NodeAddress;
    TString NodeResolveHost;
    TMaybe<TString> Path;
    bool FixedNodeID;
    ui32 InterconnectPort;
    NActors::TNodeLocation Location;
};

class INodeRegistrationResult {
public:
    virtual ~INodeRegistrationResult() {}
    virtual void Apply(NKikimrConfig::TAppConfig& appConfig, ui32& nodeId, TKikimrScopeId& scopeId) const = 0;
};

class INodeBrokerClient {
public:
    virtual ~INodeBrokerClient() {}
    virtual std::unique_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& regSettings,
        const IEnv& env) const = 0;
};

// ===

struct TDynConfigSettings {
    ui32 NodeId;
    TString DomainName;
    TString TenantName;
    TString FQDNHostName;
    TString NodeType;
    TString StaffApiUserToken;
};

class IDynConfigClient {
public:
    virtual ~IDynConfigClient() {}
    virtual TMaybe<NKikimr::NClient::TConfigurationResult> GetConfig(
        const TGrpcSslSettings& gs,
        const TVector<TString>& addrs,
        const TDynConfigSettings& settings,
        const IEnv& env) const = 0;
};

// ===

class IInitialConfigurator {
public:
    virtual ~IInitialConfigurator() {};
    virtual void RegisterCliOptions(NLastGetopt::TOpts& opts) = 0;
    virtual void ValidateOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) = 0;
    virtual void Parse(const TVector<TString>& freeArgs) = 0;
};

std::unique_ptr<IConfigUpdateTracer> MakeDefaultConfigUpdateTracer();
std::unique_ptr<IProtoConfigFileProvider> MakeDefaultProtoConfigFileProvider();
std::unique_ptr<IEnv> MakeDefaultEnv();
std::unique_ptr<IErrorCollector> MakeDefaultErrorCollector();
std::unique_ptr<IMemLogInitializer> MakeDefaultMemLogInitializer();
std::unique_ptr<INodeBrokerClient> MakeDefaultNodeBrokerClient();

std::unique_ptr<IInitialConfigurator> MakeDefaultInitialConfigurator(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IMemLogInitializer& memLogInit,
        NConfig::INodeBrokerClient& nodeBrokerClient,
        NConfig::IEnv& env);

class TInitialConfigurator {
public:
    TInitialConfigurator(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IMemLogInitializer& memLogInit,
        NConfig::INodeBrokerClient& nodeBrokerClient,
        NConfig::IEnv& env)
            : Impl(MakeDefaultInitialConfigurator(
                       errorCollector,
                       protoConfigFileProvider,
                       configUpdateTracer,
                       memLogInit,
                       nodeBrokerClient,
                       env))
    {}

    void RegisterCliOptions(NLastGetopt::TOpts& opts) {
        Impl->RegisterCliOptions(opts);
    }

    void ValidateOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) {
        Impl->ValidateOptions(opts, parseResult);
    }

    void Parse(const TVector<TString>& freeArgs) {
        Impl->Parse(freeArgs);
    }

private:
    std::unique_ptr<IInitialConfigurator> Impl;
};

} // namespace NKikimr::NConfig
