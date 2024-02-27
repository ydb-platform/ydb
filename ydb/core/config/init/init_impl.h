#pragma once

#include "init.h"

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/core/base/location.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/config_item_info.h>
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/public/lib/ydb_cli/common/common.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/getopt/small/last_getopt_opts.h>

#include <util/system/hostname.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/generic/maybe.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>

#include <filesystem>
#include <variant>

namespace fs = std::filesystem;

extern TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
extern TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();

using namespace NYdb::NConsoleClient;

namespace NKikimr::NConfig {

struct TCallContext {
    const char* File;
    int Line;
};

#define TRACE_CONFIG_CHANGE(CHANGE_CONTEXT, KIND, CHANGE_KIND) \
    ConfigUpdateTracer.Add(KIND, TConfigItemInfo::TUpdate{CHANGE_CONTEXT.File, static_cast<ui32>(CHANGE_CONTEXT.Line), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define TRACE_CONFIG_CHANGE_INPLACE(KIND, CHANGE_KIND) \
    ConfigUpdateTracer.Add(KIND, TConfigItemInfo::TUpdate{__FILE__, static_cast<ui32>(__LINE__), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define TRACE_CONFIG_CHANGE_INPLACE_T(KIND, CHANGE_KIND) \
    ConfigUpdateTracer.Add(NKikimrConsole::TConfigItem:: KIND ## Item, TConfigItemInfo::TUpdate{__FILE__, static_cast<ui32>(__LINE__), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define CALL_CTX() ::NKikimr::NConfig::TCallContext{__FILE__, __LINE__}

constexpr TStringBuf NODE_KIND_YDB = "ydb";
constexpr TStringBuf NODE_KIND_YQ = "yq";

constexpr static ui32 DefaultLogLevel = NActors::NLog::PRI_WARN; // log settings
constexpr static ui32 DefaultLogSamplingLevel = NActors::NLog::PRI_DEBUG; // log settings
constexpr static ui32 DefaultLogSamplingRate = 0; // log settings

template<typename T>
bool ParsePBFromString(const TString &content, T *pb, bool allowUnknown = false) {
    if (!allowUnknown) {
        return ::google::protobuf::TextFormat::ParseFromString(content, pb);
    }

    ::google::protobuf::TextFormat::Parser parser;
    parser.AllowUnknownField(true);
    return parser.ParseFromString(content, pb);
}

struct TConfigRefs {
    IConfigUpdateTracer& Tracer;
    IErrorCollector& ErrorCollector;
    IProtoConfigFileProvider& ProtoConfigFileProvider;
};


template <class TProto>
using TAccessors = std::tuple<
        bool (NKikimrConfig::TAppConfig::*)() const,
        const TProto& (NKikimrConfig::TAppConfig::*)() const,
        TProto* (NKikimrConfig::TAppConfig::*)()
    >;

template <class TFieldTag>
auto MutableConfigPart(
        TConfigRefs refs,
        const char *optname,
        TFieldTag tag,
        NKikimrConfig::TAppConfig& baseConfig,
        NKikimrConfig::TAppConfig& appConfig,
        TCallContext callCtx) -> decltype((appConfig.*std::get<2>(NKikimrConfig::TAppConfig::GetFieldAccessorsByFieldTag(tag)))())
{
    auto [hasConfig, getConfig, mutableConfig] = NKikimrConfig::TAppConfig::GetFieldAccessorsByFieldTag(tag);
    ui32 kind = NKikimrConfig::TAppConfig::GetFieldIdByFieldTag(tag);

    auto& ConfigUpdateTracer = refs.Tracer;
    auto& errorCollector = refs.ErrorCollector;
    auto& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    if ((appConfig.*hasConfig)()) {
        return nullptr; // this field is already provided in AppConfig, so we don't overwrite it
    }

    if (optname && protoConfigFileProvider.Has(optname)) {
        auto *res = (appConfig.*mutableConfig)();

        TString path = protoConfigFileProvider.Get(optname);
        const TString protoString = protoConfigFileProvider.GetProtoFromFile(path, errorCollector);
        // TODO(Enjeciton): CFG-UX-0 handle error collector errors
        const bool result = ParsePBFromString(protoString, res);
        if (!result) {
            errorCollector.Fatal(Sprintf("Can't parse protobuf: %s", path.c_str()));
            return nullptr;
        }

        TRACE_CONFIG_CHANGE(callCtx, kind, MutableConfigPartFromFile);

        return res;
    } else if ((baseConfig.*hasConfig)()) {
        auto* res = (appConfig.*mutableConfig)();
        res->CopyFrom((baseConfig.*getConfig)());
        TRACE_CONFIG_CHANGE(callCtx, kind, MutableConfigPartFromBaseConfig);
        return res;
    }

    return nullptr;
}

template<typename TFieldTag>
auto MutableConfigPartMerge(
    TConfigRefs refs,
    const char *optname,
    TFieldTag tag,
    NKikimrConfig::TAppConfig& appConfig,
    TCallContext callCtx) -> decltype((appConfig.*std::get<2>(NKikimrConfig::TAppConfig::GetFieldAccessorsByFieldTag(tag)))())
{
    auto mutableConfig = std::get<2>(NKikimrConfig::TAppConfig::GetFieldAccessorsByFieldTag(tag));
    ui32 kind = NKikimrConfig::TAppConfig::GetFieldIdByFieldTag(tag);

    auto& ConfigUpdateTracer = refs.Tracer;
    auto& errorCollector = refs.ErrorCollector;
    auto& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    if (protoConfigFileProvider.Has(optname)) {
        typename std::remove_reference<decltype(*(appConfig.*mutableConfig)())>::type cfg;

        TString path = protoConfigFileProvider.Get(optname);
        const TString protoString = protoConfigFileProvider.GetProtoFromFile(path, errorCollector);
        // TODO(Enjection): CFG-UX-0 handle error collector errors
        const bool result = ParsePBFromString(protoString, &cfg);
        if (!result) {
            errorCollector.Fatal(Sprintf("Can't parse protobuf: %s", path.c_str()));
            return nullptr;
        }

        auto *res = (appConfig.*mutableConfig)();
        res->MergeFrom(cfg);
        TRACE_CONFIG_CHANGE(callCtx, kind, MutableConfigPartMergeFromFile);
        return res;
    }

    return nullptr;
}

void AddProtoConfigOptions(IProtoConfigFileProvider& out);
void LoadBootstrapConfig(IProtoConfigFileProvider& protoConfigFileProvider, IErrorCollector& errorCollector, TVector<TString> configFiles, NKikimrConfig::TAppConfig& out);
void LoadYamlConfig(TConfigRefs refs, const TString& yamlConfigFile, NKikimrConfig::TAppConfig& appConfig, TCallContext callCtx);
void CopyNodeLocation(NActorsInterconnect::TNodeLocation* dst, const NYdb::NDiscovery::TNodeLocation& src);
void CopyNodeLocation(NYdb::NDiscovery::TNodeLocation* dst, const NActorsInterconnect::TNodeLocation& src);

template <class TType>
struct TWithDefault {
    using TWrappedType = TType;
    TType Value;
    bool Default = false;

    constexpr explicit operator bool() const {
        return !Default;
    }

    void EnsureDefined() const {
        if (Y_UNLIKELY(!Default)) {
            ythrow yexception() << "TWithDefault access through GetRef() assuming it is non-default";
        }
    }

    constexpr const TType& GetRef() const& {
        EnsureDefined();

        return Value;
    }

    constexpr TType& GetRef()& {
        EnsureDefined();

        return Value;
    }

    constexpr const TType&& GetRef() const&& {
        EnsureDefined();

        return std::move(Value);
    }

    constexpr const TType& operator*() const& {
        return Value;
    }

    constexpr TType& operator*() & {
        return Value;
    }
};

template <class TType>
class TWithDefaultOptHandler
  : public NLastGetopt::IOptHandler
{
public:
    TWithDefaultOptHandler(TType* target)
        : Target(target)
    {}

    void HandleOpt(const NLastGetopt::TOptsParser* parser) override {
        const auto* curOpt = parser->CurOpt();
        TStringBuf val(parser->CurValStr());
        try {
            if (!val.IsInited() || parser->CurVal() == curOpt->GetDefaultValue().Data()) {
                Target->Value = FromString<typename TType::TWrappedType>(curOpt->GetDefaultValue());
                Target->Default = true;
                return;
            }
            Target->Value = FromString<typename TType::TWrappedType>(val);
        } catch (...) {
            ythrow NLastGetopt::TUsageException() << "failed to parse opt " << curOpt->ToShortString() << " value " << TString(val).Quote() << ": " << CurrentExceptionMessage();
        }
    }

private:
    TType* Target;
};

template <>
class TWithDefaultOptHandler<TWithDefault<TString>>
  : public NLastGetopt::IOptHandler
{
public:
    TWithDefaultOptHandler(TWithDefault<TString>* target)
        : Target(target)
    {}

    void HandleOpt(const NLastGetopt::TOptsParser* parser) override {
        const auto* curOpt = parser->CurOpt();
        TStringBuf val(parser->CurValStr());
        if (!val.IsInited() || parser->CurVal() == curOpt->GetDefaultValue().Data()) {
            Target->Value = curOpt->GetDefaultValue();
            Target->Default = true;
            return;
        }
        Target->Value = val;
    }

private:
    TWithDefault<TString>* Target;
};

struct TCommonAppOptions {
    TWithDefault<ui32> LogLevel; // log settings
    TWithDefault<ui32> LogSamplingLevel; // log settings
    TWithDefault<ui32> LogSamplingRate; // log settings
    TWithDefault<TString> LogFormat;// log settings
    TMaybe<TString> SysLogServiceTag; // unique tags for sys logs
    TMaybe<TString> LogFileName; // log file name to initialize file log backend
    TWithDefault<TString> ClusterName; // log settings

    ui32 NodeId = 0;
    TMaybe<TString> NodeIdValue;
    ui32 DefaultInterconnectPort = 19001;
    ui32 MonitoringPort = 0;
    TString MonitoringAddress;
    ui32 MonitoringThreads = 10;
    TString MonitoringCertificateFile;
    TString RestartsCountFile = "";
    size_t CompileInflightLimit = 100000; // MiniKQLCompileService
    TString UDFsDir;
    TVector<TString> UDFsPaths;
    TMaybe<TString> TenantName;
    TVector<TString> NodeBrokerAddresses;
    ui32 NodeBrokerPort = 0;
    bool NodeBrokerUseTls = false;
    bool FixedNodeID = false;
    ui32 InterconnectPort = 0;
    bool IgnoreCmsConfigs = false;
    bool TinyMode = false;
    TString NodeAddress;
    TString NodeHost;
    TString NodeResolveHost;
    TString NodeDomain;
    ui32 SqsHttpPort = 0;
    TString NodeKind = TString(NODE_KIND_YDB);
    TMaybe<TString> NodeType;
    TMaybe<TString> DataCenter;
    TString Rack = "";
    ui32 Body = 0;
    ui32 GRpcPort = 0;
    ui32 GRpcsPort = 0;
    TString GRpcPublicHost = "";
    ui32 GRpcPublicPort = 0;
    ui32 GRpcsPublicPort = 0;
    TVector<TString> GRpcPublicAddressesV4;
    TVector<TString> GRpcPublicAddressesV6;
    TString GRpcPublicTargetNameOverride = "";
    TGrpcSslSettings GrpcSslSettings;
    TString PathToInterconnectCertFile;
    TString PathToInterconnectPrivateKeyFile;
    TString PathToInterconnectCaFile;
    TString YamlConfigFile;
    bool SysLogEnabled = false;
    bool TcpEnabled = false;
    bool SuppressVersionCheck = false;

    void RegisterCliOptions(NLastGetopt::TOpts& opts) {
        opts.AddLongOption("cluster-name", "which cluster this node belongs to")
            .DefaultValue("unknown").OptionalArgument("STR")
            .Handler(new TWithDefaultOptHandler(&ClusterName));
        opts.AddLongOption("log-level", "default logging level").OptionalArgument("1-7")
            .DefaultValue(ToString(DefaultLogLevel))
            .Handler(new TWithDefaultOptHandler(&LogLevel));
        opts.AddLongOption("log-sampling-level", "sample logs equal to or above this level").OptionalArgument("1-7")
            .DefaultValue(ToString(DefaultLogSamplingLevel))
            .Handler(new TWithDefaultOptHandler(&LogSamplingLevel));
        opts.AddLongOption("log-sampling-rate",
                           "log only each Nth message with priority matching sampling level; 0 turns log sampling off")
            .OptionalArgument(Sprintf("0,%" PRIu32, Max<ui32>()))
            .DefaultValue(ToString(DefaultLogSamplingRate))
            .Handler(new TWithDefaultOptHandler(&LogSamplingRate));
        opts.AddLongOption("log-format", "log format to use; short skips the priority and timestamp")
            .DefaultValue("full").OptionalArgument("full|short|json")
            .Handler(new TWithDefaultOptHandler(&LogFormat));
        opts.AddLongOption("syslog", "send to syslog instead of stderr").SetFlag(&SysLogEnabled);
        opts.AddLongOption("syslog-service-tag", "unique tag for syslog").RequiredArgument("NAME").StoreResult(&SysLogServiceTag);
        opts.AddLongOption("log-file-name", "file name for log backend").RequiredArgument("NAME").StoreResult(&LogFileName);
        opts.AddLongOption("tcp", "start tcp interconnect").NoArgument().SetFlag(&TcpEnabled);
        opts.AddLongOption('n', "node", "Node ID or 'static' to auto-detect using naming file and ic-port.")
            .RequiredArgument("[NUM|static]").StoreResult(&NodeIdValue);
        opts.AddLongOption("node-broker", "node broker address host:port")
            .RequiredArgument("ADDR").AppendTo(&NodeBrokerAddresses);
        opts.AddLongOption("node-broker-port", "node broker port (hosts from naming file are used)")
            .RequiredArgument("PORT").StoreResult(&NodeBrokerPort);
        opts.AddLongOption("node-broker-use-tls", "use tls for node broker (hosts from naming file are used)")
            .RequiredArgument("PORT").StoreResult(&NodeBrokerUseTls);
        opts.AddLongOption("node-address", "address for dynamic node")
            .RequiredArgument("ADDR").StoreResult(&NodeAddress);
        opts.AddLongOption("node-host", "hostname for dynamic node")
            .RequiredArgument("NAME").StoreResult(&NodeHost);
        opts.AddLongOption("node-resolve-host", "resolve hostname for dynamic node")
            .RequiredArgument("NAME").StoreResult(&NodeResolveHost);
        opts.AddLongOption("node-domain", "domain for dynamic node to register in")
            .RequiredArgument("NAME").StoreResult(&NodeDomain);
        opts.AddLongOption("ic-port", "interconnect port")
            .RequiredArgument("NUM").StoreResult(&InterconnectPort);
        opts.AddLongOption("sqs-port", "sqs port")
            .RequiredArgument("NUM").StoreResult(&SqsHttpPort);
        opts.AddLongOption("tenant", "add binding for Local service to specified tenant, might be one of {'/<root>', '/<root>/<path_to_user>'}")
            .RequiredArgument("NAME").StoreResult(&TenantName);
        opts.AddLongOption("mon-port", "Monitoring port").OptionalArgument("NUM").StoreResult(&MonitoringPort);
        opts.AddLongOption("mon-address", "Monitoring address").OptionalArgument("ADDR").StoreResult(&MonitoringAddress);
        opts.AddLongOption("mon-cert", "Monitoring certificate (https)").OptionalArgument("PATH").StoreResult(&MonitoringCertificateFile);
        opts.AddLongOption("mon-threads", "Monitoring http server threads").RequiredArgument("NUM").StoreResult(&MonitoringThreads);
        opts.AddLongOption("suppress-version-check", "Suppress version compatibility checking via IC").NoArgument().SetFlag(&SuppressVersionCheck);

        opts.AddLongOption("grpc-port", "enable gRPC server on port").RequiredArgument("PORT").StoreResult(&GRpcPort);
        opts.AddLongOption("grpcs-port", "enable gRPC SSL server on port").RequiredArgument("PORT").StoreResult(&GRpcsPort);
        opts.AddLongOption("grpc-public-host", "set public gRPC host for discovery").RequiredArgument("HOST").StoreResult(&GRpcPublicHost);
        opts.AddLongOption("grpc-public-port", "set public gRPC port for discovery").RequiredArgument("PORT").StoreResult(&GRpcPublicPort);
        opts.AddLongOption("grpcs-public-port", "set public gRPC SSL port for discovery").RequiredArgument("PORT").StoreResult(&GRpcsPublicPort);
        opts.AddLongOption("grpc-public-address-v4", "set public ipv4 address for discovery").RequiredArgument("ADDR").EmplaceTo(&GRpcPublicAddressesV4);
        opts.AddLongOption("grpc-public-address-v6", "set public ipv6 address for discovery").RequiredArgument("ADDR").EmplaceTo(&GRpcPublicAddressesV6);
        opts.AddLongOption("grpc-public-target-name-override", "set public hostname override for TLS in discovery").RequiredArgument("HOST").StoreResult(&GRpcPublicTargetNameOverride);
        opts.AddLongOption('r', "restarts-count-file", "State for restarts monitoring counter,\nuse empty string to disable\n")
            .OptionalArgument("PATH").DefaultValue(RestartsCountFile)
            .StoreResult(&RestartsCountFile);
        opts.AddLongOption("compile-inflight-limit", "Limit on parallel programs compilation").OptionalArgument("NUM").StoreResult(&CompileInflightLimit);
        opts.AddLongOption("udf", "Load shared library with UDF by given path").AppendTo(&UDFsPaths);
        opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory").StoreResult(&UDFsDir);
        opts.AddLongOption("node-kind", Sprintf("Kind of the node (affects list of services activated allowed values are {'%s', '%s'} )", NODE_KIND_YDB.data(), NODE_KIND_YQ.data()))
            .RequiredArgument("NAME").StoreResult(&NodeKind);
        opts.AddLongOption("node-type", "Type of the node")
            .RequiredArgument("NAME").StoreResult(&NodeType);
        opts.AddLongOption("ignore-cms-configs", "Don't load configs from CMS")
            .NoArgument().SetFlag(&IgnoreCmsConfigs);
        opts.AddLongOption("cert", "Path to client certificate file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCertFile);
        opts.AddLongOption("grpc-cert", "Path to client certificate file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&GrpcSslSettings.PathToGrpcCertFile);
        opts.AddLongOption("ic-cert", "Path to client certificate file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCertFile);
        opts.AddLongOption("key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        opts.AddLongOption("grpc-key", "Path to private key file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&GrpcSslSettings.PathToGrpcPrivateKeyFile);
        opts.AddLongOption("ic-key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        opts.AddLongOption("ca", "Path to certificate authority file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCaFile);
        opts.AddLongOption("grpc-ca", "Path to certificate authority file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&GrpcSslSettings.PathToGrpcCaFile);
        opts.AddLongOption("ic-ca", "Path to certificate authority file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCaFile);
        opts.AddLongOption("data-center", "data center name (used to describe dynamic node location)")
            .RequiredArgument("NAME").StoreResult(&DataCenter);
        opts.AddLongOption("rack", "rack name (used to describe dynamic node location)")
            .RequiredArgument("NAME").StoreResult(&Rack);
        opts.AddLongOption("body", "body name (used to describe dynamic node location)")
            .RequiredArgument("NUM").StoreResult(&Body);
        opts.AddLongOption("yaml-config", "Yaml config").OptionalArgument("PATH").StoreResult(&YamlConfigFile);

        opts.AddLongOption("tiny-mode", "Start in a tiny mode")
            .NoArgument().SetFlag(&TinyMode);
    }

    void ApplyFields(NKikimrConfig::TAppConfig& appConfig, IEnv& env, IConfigUpdateTracer& ConfigUpdateTracer) const {
        if (!appConfig.HasAllocatorConfig()) {
            appConfig.MutableAllocatorConfig()->CopyFrom(*DummyAllocatorConfig());
            TRACE_CONFIG_CHANGE_INPLACE_T(AllocatorConfig, UpdateExplicitly);
        }

        // apply certificates, if any
        if (!PathToInterconnectCertFile.Empty()) {
            appConfig.MutableInterconnectConfig()->SetPathToCertificateFile(PathToInterconnectCertFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(InterconnectConfig, UpdateExplicitly);
        }

        if (!PathToInterconnectPrivateKeyFile.Empty()) {
            appConfig.MutableInterconnectConfig()->SetPathToPrivateKeyFile(PathToInterconnectPrivateKeyFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(InterconnectConfig, UpdateExplicitly);
        }

        if (!PathToInterconnectCaFile.Empty()) {
            appConfig.MutableInterconnectConfig()->SetPathToCaFile(PathToInterconnectCaFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(InterconnectConfig, UpdateExplicitly);
        }

        if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().HasCert()) {
            appConfig.MutableGRpcConfig()->SetPathToCertificateFile(appConfig.GetGRpcConfig().GetCert());
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!GrpcSslSettings.PathToGrpcCertFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToCertificateFile(GrpcSslSettings.PathToGrpcCertFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().HasKey()) {
            appConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(appConfig.GetGRpcConfig().GetKey());
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!GrpcSslSettings.PathToGrpcPrivateKeyFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(GrpcSslSettings.PathToGrpcPrivateKeyFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().HasCA()) {
            appConfig.MutableGRpcConfig()->SetPathToCaFile(appConfig.GetGRpcConfig().GetCA());
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!GrpcSslSettings.PathToGrpcCaFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToCaFile(GrpcSslSettings.PathToGrpcCaFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!appConfig.HasDomainsConfig()) {
            ythrow yexception() << "DomainsConfig is not provided";
        }

        if (!appConfig.HasChannelProfileConfig()) {
            ythrow yexception() << "ChannelProfileConfig is not provided";
        }

        if (NodeKind == NODE_KIND_YQ && InterconnectPort) {
            auto& fqConfig = *appConfig.MutableFederatedQueryConfig();
            auto& nmConfig = *fqConfig.MutableNodesManager();
            nmConfig.SetPort(InterconnectPort);
            nmConfig.SetHost(env.HostName());
        }

        if (SuppressVersionCheck) {
            if (appConfig.HasNameserviceConfig()) {
                appConfig.MutableNameserviceConfig()->SetSuppressVersionCheck(true);
                TRACE_CONFIG_CHANGE_INPLACE_T(NameserviceConfig, UpdateExplicitly);
            } else {
                ythrow yexception() << "--suppress-version-check option is provided without static nameservice config";
            }
        }

        // apply options affecting UDF paths
        if (!appConfig.HasUDFsDir()) {
            appConfig.SetUDFsDir(UDFsDir);
        }

        if (!appConfig.UDFsPathsSize()) {
            for (const auto& path : UDFsPaths) {
                appConfig.AddUDFsPaths(path);
            }
        }

        if (!appConfig.HasMonitoringConfig()) {
            appConfig.MutableMonitoringConfig()->SetMonitoringThreads(MonitoringThreads);
            TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);
        }
        if (!appConfig.HasRestartsCountConfig() && RestartsCountFile) {
            appConfig.MutableRestartsCountConfig()->SetRestartsCountFile(RestartsCountFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(RestartsCountConfig, UpdateExplicitly);
        }

        // Ports and node type are always applied (even if config was loaded from CMS).
        if (MonitoringPort) {
            appConfig.MutableMonitoringConfig()->SetMonitoringPort(MonitoringPort);
            TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);
        }
        if (MonitoringAddress) {
            appConfig.MutableMonitoringConfig()->SetMonitoringAddress(MonitoringAddress);
            TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);
        }
        if (MonitoringCertificateFile) {
            TString sslCertificate = TUnbufferedFileInput(MonitoringCertificateFile).ReadAll();
            if (!sslCertificate.empty()) {
                appConfig.MutableMonitoringConfig()->SetMonitoringCertificate(sslCertificate);
                TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);
            } else {
                ythrow yexception() << "invalid ssl certificate file";
            }
        }
        if (SqsHttpPort) {
            appConfig.MutableSqsConfig()->MutableHttpServerConfig()->SetPort(SqsHttpPort);
            TRACE_CONFIG_CHANGE_INPLACE_T(SqsConfig, UpdateExplicitly);
        }
        if (GRpcPort) {
            auto& conf = *appConfig.MutableGRpcConfig();
            conf.SetStartGRpcProxy(true);
            conf.SetPort(GRpcPort);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (GRpcsPort) {
            auto& conf = *appConfig.MutableGRpcConfig();
            conf.SetStartGRpcProxy(true);
            conf.SetSslPort(GRpcsPort);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (GRpcPublicHost) {
            auto& conf = *appConfig.MutableGRpcConfig();
            conf.SetPublicHost(GRpcPublicHost);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicHost()) {
                    ext.SetPublicHost(GRpcPublicHost);
                }
            }
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (GRpcPublicPort) {
            auto& conf = *appConfig.MutableGRpcConfig();
            conf.SetPublicPort(GRpcPublicPort);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicPort()) {
                    ext.SetPublicPort(GRpcPublicPort);
                }
            }
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (GRpcsPublicPort) {
            auto& conf = *appConfig.MutableGRpcConfig();
            conf.SetPublicSslPort(GRpcsPublicPort);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicSslPort()) {
                    ext.SetPublicSslPort(GRpcsPublicPort);
                }
            }
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        for (const auto& addr : GRpcPublicAddressesV4) {
            appConfig.MutableGRpcConfig()->AddPublicAddressesV4(addr);
        }
        if (GRpcPublicAddressesV4.size()) {
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        for (const auto& addr : GRpcPublicAddressesV6) {
            appConfig.MutableGRpcConfig()->AddPublicAddressesV6(addr);
        }
        if (GRpcPublicAddressesV6.size()) {
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (GRpcPublicTargetNameOverride) {
            appConfig.MutableGRpcConfig()->SetPublicTargetNameOverride(GRpcPublicTargetNameOverride);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }
        if (NodeType) {
            appConfig.MutableTenantPoolConfig()->SetNodeType(NodeType.GetRef());
            TRACE_CONFIG_CHANGE_INPLACE_T(TenantPoolConfig, UpdateExplicitly);
        }

        if (TenantName && InterconnectPort != DefaultInterconnectPort) {
            appConfig.MutableMonitoringConfig()->SetHostLabelOverride(HostAndICPort(env));
            TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);
        }

        if (DataCenter) {
            appConfig.MutableMonitoringConfig()->SetDataCenter(to_lower(DataCenter.GetRef()));
            TRACE_CONFIG_CHANGE_INPLACE_T(MonitoringConfig, UpdateExplicitly);

            if (appConfig.HasFederatedQueryConfig()) {
                appConfig.MutableFederatedQueryConfig()->MutableNodesManager()->SetDataCenter(to_lower(DataCenter.GetRef()));
                TRACE_CONFIG_CHANGE_INPLACE_T(FederatedQueryConfig, UpdateExplicitly);
            }
        }
    }

    ui32 DeduceNodeId(const NKikimrConfig::TAppConfig& appConfig, IEnv& env) const {
        ui32 nodeId = 0;
        if (NodeIdValue) {
            if (NodeIdValue.GetRef() == "static") {
                if (!appConfig.HasNameserviceConfig() || !InterconnectPort) {
                    ythrow yexception() << "'--node static' requires naming file and IC port to be specified";
                }

                try {
                    nodeId = FindStaticNodeId(appConfig, env);
                } catch(TSystemError& e) {
                    ythrow yexception() << "cannot detect host name: " << e.what();
                }

                if (!nodeId) {
                    ythrow yexception() << "cannot detect node ID for " << env.HostName() << ":" << InterconnectPort
                        << " and for " << env.FQDNHostName() << ":" << InterconnectPort << Endl;
                }
                return nodeId;
            } else {
                if (!TryFromString(NodeIdValue.GetRef(), nodeId)) {
                    ythrow yexception() << "wrong '--node' value (should be NUM, 'static')";
                }
            }
        }
        return nodeId;
    }

    NActors::TNodeLocation CreateNodeLocation() const {
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter(DataCenter ? DataCenter.GetRef() : TString(""));
        location.SetRack(Rack);
        location.SetUnit(ToString(Body));
        NActors::TNodeLocation loc(location);

        NActorsInterconnect::TNodeLocation legacy;
        legacy.SetDataCenterNum(DataCenterFromString(DataCenter ? DataCenter.GetRef() : TString("")));
        legacy.SetRoomNum(0);
        legacy.SetRackNum(RackFromString(Rack));
        legacy.SetBodyNum(Body);
        loc.InheritLegacyValue(TNodeLocation(legacy));
        return loc;
    }

    void ApplyLogSettings(NKikimrConfig::TAppConfig& appConfig, IConfigUpdateTracer& ConfigUpdateTracer) const {
        if (SysLogServiceTag && !appConfig.GetLogConfig().GetSysLogService()) {
            appConfig.MutableLogConfig()->SetSysLogService(SysLogServiceTag.GetRef());
            TRACE_CONFIG_CHANGE_INPLACE_T(LogConfig, UpdateExplicitly);
        }

        if (LogFileName) {
            appConfig.MutableLogConfig()->SetBackendFileName(LogFileName.GetRef());
            TRACE_CONFIG_CHANGE_INPLACE_T(LogConfig, UpdateExplicitly);
        }
    }

    ui32 FindStaticNodeId(const NKikimrConfig::TAppConfig& appConfig, IEnv& env) const {
        std::vector<TString> candidates = {env.HostName(), env.FQDNHostName()};
        for(auto& candidate: candidates) {
            candidate.to_lower();

            const NKikimrConfig::TStaticNameserviceConfig& nameserviceConfig = appConfig.GetNameserviceConfig();
            for (const auto& node : nameserviceConfig.GetNode()) {
                if (node.GetHost() == candidate && InterconnectPort == node.GetPort()) {
                    return node.GetNodeId();
                }
            }
        }

        return 0;
    }

    TString HostAndICPort(IEnv& env) const {
        try {
            auto hostname = to_lower(env.HostName());
            hostname = hostname.substr(0, hostname.find('.'));
            return TStringBuilder() << hostname << ":" << InterconnectPort;
        } catch (TSystemError& error) {
            return "";
        }
    }

    void SetupLogConfigDefaults(NKikimrConfig::TLogConfig& logConfig, IConfigUpdateTracer& ConfigUpdateTracer) const {
        if (SysLogEnabled) {
            logConfig.SetSysLog(true);
        }
        if (LogLevel) {
            logConfig.SetDefaultLevel(LogLevel.GetRef());
        }
        if (LogSamplingLevel) {
            logConfig.SetDefaultSamplingLevel(LogSamplingLevel.GetRef());
        }
        if (LogSamplingRate) {
            logConfig.SetDefaultSamplingRate(LogSamplingRate.GetRef());
        }
        if (LogFormat) {
            logConfig.SetFormat(LogFormat.GetRef());
        }
        if (ClusterName) {
            logConfig.SetClusterName(ClusterName.GetRef());
        }
        TRACE_CONFIG_CHANGE_INPLACE_T(LogConfig, UpdateExplicitly);
    }

    void SetupBootstrapConfigDefaults(NKikimrConfig::TBootstrap& bootstrapConfig, IConfigUpdateTracer& ConfigUpdateTracer) const {
        bootstrapConfig.MutableCompileServiceConfig()->SetInflightLimit(CompileInflightLimit);
        TRACE_CONFIG_CHANGE_INPLACE_T(BootstrapConfig, UpdateExplicitly);
    };

    void SetupInterconnectConfigDefaults(NKikimrConfig::TInterconnectConfig& icConfig, IConfigUpdateTracer& ConfigUpdateTracer) const {
        if (TcpEnabled) {
            icConfig.SetStartTcp(true);
            TRACE_CONFIG_CHANGE_INPLACE_T(InterconnectConfig, UpdateExplicitly);
        }
    };

    void FillClusterEndpoints(const NKikimrConfig::TAppConfig& appConfig, TVector<TString> &addrs) const {
        if (!NodeBrokerAddresses.empty()) {
            for (auto addr: NodeBrokerAddresses) {
                addrs.push_back(addr);
            }
        } else {
            if (!NodeBrokerPort) {
                ythrow yexception() << "NodeBrokerPort MUST be defined";
            }

            for (const auto &node : appConfig.GetNameserviceConfig().GetNode()) {
                addrs.emplace_back(TStringBuilder() << (NodeBrokerUseTls ? "grpcs://" : "") << node.GetHost() << ':' << NodeBrokerPort);
            }
        }
        ShuffleRange(addrs);
    }

    TMaybe<TString> GetSchemePath() const {
        if (TenantName && TenantName.GetRef().StartsWith('/')) {
            return TenantName.GetRef(); // TODO(alexvru): fix it
        }
        return {};
    }

    void ValidateTenant() const {
        if (TenantName) {
            if (!IsStartWithSlash(TenantName.GetRef())) { // ?
                ythrow yexception() << "leading / in --tenant parametr is always required.";
            }
            if (NodeId && NodeKind != NODE_KIND_YQ) {
                ythrow yexception() << "opt '--node' compatible only with '--tenant no', opt 'node' incompatible with any other values of opt '--tenant'";
            }
        }
    }

    void ApplyServicesMask(NKikimr::TBasicKikimrServicesMask& out) const {
        if (NodeKind == NODE_KIND_YDB) {
            if (TinyMode) {
                out.SetTinyMode();
            }
            // do nothing => default behaviour
        } else if (NodeKind == NODE_KIND_YQ) {
            out.DisableAll();
            out.EnableYQ();
        } else {
            ythrow yexception() << "wrong '--node-kind' value '" << NodeKind << "', only '" << NODE_KIND_YDB << "' or '" << NODE_KIND_YQ << "' is allowed";
        }
    }

    bool IsStaticNode() const {
        return NodeBrokerAddresses.empty() && !NodeBrokerPort;
    }

    void ValidateStaticNodeConfig() const {
        if (!NodeId) {
            ythrow yexception() << "Either --node [NUM|'static'] or --node-broker[-port] should be specified";
        }
    }
};

struct TMbusAppOptions {
    ui32 BusProxyPort = NMsgBusProxy::TProtocol::DefaultPort;
    NBus::TBusQueueConfig ProxyBusQueueConfig;
    NBus::TBusServerSessionConfig ProxyBusSessionConfig;
    TString TracePath;
    TVector<ui64> ProxyBindToProxy;
    bool Start = false;

    void RegisterCliOptions(NLastGetopt::TOpts& opts) {
        opts.AddLongOption("mbus", "Start MessageBus proxy").NoArgument().SetFlag(&Start);
        opts.AddLongOption("mbus-port", "MessageBus proxy port").RequiredArgument("PORT").StoreResult(&BusProxyPort);
        opts.AddLongOption("mbus-trace-path", "Path for trace files").RequiredArgument("PATH").StoreResult(&TracePath);
        opts.AddLongOption("proxy", "Bind to proxy(-ies)").RequiredArgument("ADDR").AppendTo(&ProxyBindToProxy);
        SetMsgBusDefaults(ProxyBusSessionConfig, ProxyBusQueueConfig);
        ProxyBusSessionConfig.ConfigureLastGetopt(opts, "mbus-");
        ProxyBusQueueConfig.ConfigureLastGetopt(opts, "mbus-");
    }

    void ValidateCliOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) const {
         if (!Start) {
            for (const auto &option : opts.Opts_) {
                for (const TString &longName : option->GetLongNames()) {
                    if (longName.StartsWith("mbus-") && parseResult.Has(option.Get())) {
                        ythrow yexception() << "option --" << longName << " is useless without --mbus option";
                    }
                }
            }
        }
    }

    void InitMessageBusConfig(NKikimrConfig::TAppConfig& appConfig) const {
        auto messageBusConfig = appConfig.MutableMessageBusConfig();
        messageBusConfig->SetStartBusProxy(Start);
        messageBusConfig->SetBusProxyPort(BusProxyPort);

        auto queueConfig = messageBusConfig->MutableProxyBusQueueConfig();
        queueConfig->SetName(ProxyBusQueueConfig.Name);
        queueConfig->SetNumWorkers(ProxyBusQueueConfig.NumWorkers);

        auto sessionConfig = messageBusConfig->MutableProxyBusSessionConfig();

        sessionConfig->SetName(ProxyBusSessionConfig.Name);
        sessionConfig->SetNumRetries(ProxyBusSessionConfig.NumRetries);
        sessionConfig->SetRetryInterval(ProxyBusSessionConfig.RetryInterval);
        sessionConfig->SetReconnectWhenIdle(ProxyBusSessionConfig.ReconnectWhenIdle);
        sessionConfig->SetMaxInFlight(ProxyBusSessionConfig.MaxInFlight);
        sessionConfig->SetPerConnectionMaxInFlight(ProxyBusSessionConfig.PerConnectionMaxInFlight);
        sessionConfig->SetPerConnectionMaxInFlightBySize(ProxyBusSessionConfig.PerConnectionMaxInFlightBySize);
        sessionConfig->SetMaxInFlightBySize(ProxyBusSessionConfig.MaxInFlightBySize);
        sessionConfig->SetTotalTimeout(ProxyBusSessionConfig.TotalTimeout);
        sessionConfig->SetSendTimeout(ProxyBusSessionConfig.SendTimeout);
        sessionConfig->SetConnectTimeout(ProxyBusSessionConfig.ConnectTimeout);
        sessionConfig->SetDefaultBufferSize(ProxyBusSessionConfig.DefaultBufferSize);
        sessionConfig->SetMaxBufferSize(ProxyBusSessionConfig.MaxBufferSize);
        sessionConfig->SetSocketRecvBufferSize(ProxyBusSessionConfig.SocketRecvBufferSize);
        sessionConfig->SetSocketSendBufferSize(ProxyBusSessionConfig.SocketSendBufferSize);
        sessionConfig->SetSocketToS(ProxyBusSessionConfig.SocketToS);
        sessionConfig->SetSendThreshold(ProxyBusSessionConfig.SendThreshold);
        sessionConfig->SetCork(ProxyBusSessionConfig.Cork.MilliSeconds());
        sessionConfig->SetMaxMessageSize(ProxyBusSessionConfig.MaxMessageSize);
        sessionConfig->SetTcpNoDelay(ProxyBusSessionConfig.TcpNoDelay);
        sessionConfig->SetTcpCork(ProxyBusSessionConfig.TcpCork);
        sessionConfig->SetExecuteOnMessageInWorkerPool(ProxyBusSessionConfig.ExecuteOnMessageInWorkerPool);
        sessionConfig->SetExecuteOnReplyInWorkerPool(ProxyBusSessionConfig.ExecuteOnReplyInWorkerPool);
        sessionConfig->SetListenPort(ProxyBusSessionConfig.ListenPort);

        for (auto proxy : ProxyBindToProxy) {
            messageBusConfig->AddProxyBindToProxy(proxy);
        }
        messageBusConfig->SetStartTracingBusProxy(!!TracePath);
        messageBusConfig->SetTracePath(TracePath);
    }
};

// =====

TString DeduceNodeDomain(const NConfig::TCommonAppOptions& cf, const NKikimrConfig::TAppConfig& appConfig);
ui32 NextValidKind(ui32 kind);
bool HasCorrespondingManagedKind(ui32 kind, const NKikimrConfig::TAppConfig& appConfig);
NClient::TKikimr GetKikimr(const TGrpcSslSettings& cf, const TString& addr, const IEnv& env);
NKikimrConfig::TAppConfig GetYamlConfigFromResult(const NKikimr::NClient::TConfigurationResult& result, const TMap<TString, TString>& labels);
NKikimrConfig::TAppConfig GetActualDynConfig(
    const NKikimrConfig::TAppConfig& yamlConfig,
    const NKikimrConfig::TAppConfig& regularConfig,
    IConfigUpdateTracer& ConfigUpdateTracer);

// =====

struct TAppInitDebugInfo {
    NKikimrConfig::TAppConfig OldConfig;
    NKikimrConfig::TAppConfig YamlConfig;
    THashMap<ui32, TConfigItemInfo> ConfigTransformInfo;
};

class TInitialConfiguratorImpl
    : public IInitialConfigurator
{
    ui32 NodeId = 0;
    TBasicKikimrServicesMask ServicesMask;
    TKikimrScopeId ScopeId;
    TString TenantName;
    TString ClusterName;

    TMap<TString, TString> Labels;

    NKikimrConfig::TAppConfig BaseConfig;
    NKikimrConfig::TAppConfig AppConfig;

    NConfig::TCommonAppOptions CommonAppOptions;
    NConfig::TMbusAppOptions MbusAppOptions;

    TAppInitDebugInfo InitDebug;

    NConfig::IErrorCollector& ErrorCollector;
    NConfig::IProtoConfigFileProvider& ProtoConfigFileProvider;
    NConfig::IConfigUpdateTracer& ConfigUpdateTracer;
    NConfig::IMemLogInitializer& MemLogInit;
    NConfig::INodeBrokerClient& NodeBrokerClient;
    NConfig::IDynConfigClient& DynConfigClient;
    NConfig::IEnv& Env;

public:
    TInitialConfiguratorImpl(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IMemLogInitializer& memLogInit,
        NConfig::INodeBrokerClient& nodeBrokerClient,
        NConfig::IDynConfigClient& dynConfigClient,
        NConfig::IEnv& env)
            : ErrorCollector(errorCollector)
            , ProtoConfigFileProvider(protoConfigFileProvider)
            , ConfigUpdateTracer(configUpdateTracer)
            , MemLogInit(memLogInit)
            , NodeBrokerClient(nodeBrokerClient)
            , DynConfigClient(dynConfigClient)
            , Env(env)
    {}

    void ValidateOptions(const NLastGetopt::TOpts& opts, const NLastGetopt::TOptsParseResult& parseResult) override {
        MbusAppOptions.ValidateCliOptions(opts, parseResult);
    }

    void Parse(const TVector<TString>& freeArgs) override {
        using TCfg = NKikimrConfig::TAppConfig;

        NConfig::TConfigRefs refs{ConfigUpdateTracer, ErrorCollector, ProtoConfigFileProvider};

        Option("auth-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());
        LoadBootstrapConfig(ProtoConfigFileProvider, ErrorCollector, freeArgs, BaseConfig);
        LoadYamlConfig(refs, CommonAppOptions.YamlConfigFile, AppConfig, CALL_CTX());
        OptionMerge("auth-token-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());

        // start memorylog as soon as possible
        Option("memorylog-file", TCfg::TMemoryLogConfigFieldTag{}, &TInitialConfiguratorImpl::InitMemLog, CALL_CTX());
        Option("naming-file", TCfg::TNameserviceConfigFieldTag{}, CALL_CTX());

        CommonAppOptions.NodeId = CommonAppOptions.DeduceNodeId(AppConfig, Env);
        Cout << "Determined node ID: " << CommonAppOptions.NodeId << Endl;

        CommonAppOptions.ValidateTenant();

        CommonAppOptions.ApplyServicesMask(ServicesMask);

        PreFillLabels(CommonAppOptions);

        if (CommonAppOptions.IsStaticNode()) {
            InitStaticNode();
        } else {
            InitDynamicNode();
        }

        LoadYamlConfig(refs, CommonAppOptions.YamlConfigFile, AppConfig, CALL_CTX());

        Option("sys-file", TCfg::TActorSystemConfigFieldTag{}, CALL_CTX());

        if (!AppConfig.HasActorSystemConfig()) {
            AppConfig.MutableActorSystemConfig()->CopyFrom(*DummyActorSystemConfig());
            TRACE_CONFIG_CHANGE_INPLACE_T(ActorSystemConfig, SetExplicitly);
        }

        Option("domains-file", TCfg::TDomainsConfigFieldTag{}, CALL_CTX());
        Option("bs-file", TCfg::TBlobStorageConfigFieldTag{}, CALL_CTX());
        Option("log-file", TCfg::TLogConfigFieldTag{}, &TInitialConfiguratorImpl::SetupLogConfigDefaults, CALL_CTX());

        // This flag is set per node and we prefer flag over CMS.
        CommonAppOptions.ApplyLogSettings(AppConfig, ConfigUpdateTracer);

        Option("ic-file", TCfg::TInterconnectConfigFieldTag{}, &TInitialConfiguratorImpl::SetupInterconnectConfigDefaults, CALL_CTX());
        Option("channels-file", TCfg::TChannelProfileConfigFieldTag{}, CALL_CTX());
        Option("bootstrap-file", TCfg::TBootstrapConfigFieldTag{}, &TInitialConfiguratorImpl::SetupBootstrapConfigDefaults, CALL_CTX());
        Option("vdisk-file", TCfg::TVDiskConfigFieldTag{}, CALL_CTX());
        Option("drivemodel-file", TCfg::TDriveModelConfigFieldTag{}, CALL_CTX());
        Option("grpc-file", TCfg::TGRpcConfigFieldTag{}, CALL_CTX());
        Option("dyn-nodes-file", TCfg::TDynamicNameserviceConfigFieldTag{}, CALL_CTX());
        Option("cms-file", TCfg::TCmsConfigFieldTag{}, CALL_CTX());
        Option("pq-file", TCfg::TPQConfigFieldTag{}, CALL_CTX());
        Option("pqcd-file", TCfg::TPQClusterDiscoveryConfigFieldTag{}, CALL_CTX());
        Option("netclassifier-file", TCfg::TNetClassifierConfigFieldTag{}, CALL_CTX());
        Option("auth-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());
        OptionMerge("auth-token-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());
        Option("key-file", TCfg::TKeyConfigFieldTag{}, CALL_CTX());
        Option("pdisk-key-file", TCfg::TPDiskKeyConfigFieldTag{}, CALL_CTX());
        Option("sqs-file", TCfg::TSqsConfigFieldTag{}, CALL_CTX());
        Option("http-proxy-file", TCfg::THttpProxyConfigFieldTag{}, CALL_CTX());
        Option("public-http-file", TCfg::TPublicHttpConfigFieldTag{}, CALL_CTX());
        Option("feature-flags-file", TCfg::TFeatureFlagsFieldTag{}, CALL_CTX());
        Option("rb-file", TCfg::TResourceBrokerConfigFieldTag{}, CALL_CTX());
        Option("metering-file", TCfg::TMeteringConfigFieldTag{}, CALL_CTX());
        Option("audit-file", TCfg::TAuditConfigFieldTag{}, CALL_CTX());
        Option("kqp-file", TCfg::TKQPConfigFieldTag{}, CALL_CTX());
        Option("incrhuge-file", TCfg::TIncrHugeConfigFieldTag{}, CALL_CTX());
        Option("alloc-file", TCfg::TAllocatorConfigFieldTag{}, CALL_CTX());
        Option("fq-file", TCfg::TFederatedQueryConfigFieldTag{}, CALL_CTX());
        Option(nullptr, TCfg::TTracingConfigFieldTag{}, CALL_CTX());
        Option(nullptr, TCfg::TFailureInjectionConfigFieldTag{}, CALL_CTX());

        CommonAppOptions.ApplyFields(AppConfig, Env, ConfigUpdateTracer);

       // MessageBus options.
        if (!AppConfig.HasMessageBusConfig()) {
            MbusAppOptions.InitMessageBusConfig(AppConfig);
            TRACE_CONFIG_CHANGE_INPLACE_T(MessageBusConfig, UpdateExplicitly);
        }

        TenantName = FillTenantPoolConfig(CommonAppOptions);

        Cout << "configured" << Endl;

        FillData(CommonAppOptions);
    }

    void FillData(const NConfig::TCommonAppOptions& cf) {
        if (cf.TenantName && ScopeId.IsEmpty()) {
            const TString myDomain = DeduceNodeDomain(cf, AppConfig);
            for (const auto& domain : AppConfig.GetDomainsConfig().GetDomain()) {
                if (domain.GetName() == myDomain) {
                    ScopeId = TKikimrScopeId(0, domain.GetDomainId());
                    break;
                }
            }
        }

        if (cf.NodeId) {
            NodeId = cf.NodeId;

            Labels["node_id"] = ToString(NodeId);
            AddLabelToAppConfig("node_id", Labels["node_id"]);
        }

        InitDebug.ConfigTransformInfo = ConfigUpdateTracer.Dump();
        ClusterName = AppConfig.GetNameserviceConfig().GetClusterUUID();
    }

    TString FillTenantPoolConfig(const NConfig::TCommonAppOptions& cf) {
        auto &slot = *AppConfig.MutableTenantPoolConfig()->AddSlots();
        slot.SetId("static-slot");
        slot.SetIsDynamic(false);
        TString tenantName = cf.TenantName ? cf.TenantName.GetRef() : CanonizePath(DeduceNodeDomain(cf, AppConfig));
        slot.SetTenantName(tenantName);
        return tenantName;
    }

    void SetupLogConfigDefaults(NKikimrConfig::TLogConfig& logConfig) {
        CommonAppOptions.SetupLogConfigDefaults(logConfig, ConfigUpdateTracer);
    }

    void AddLabelToAppConfig(const TString& name, const TString& value) {
        for (auto &label : *AppConfig.MutableLabels()) {
            if (label.GetName() == name) {
                label.SetValue(value);
                return;
            }
        }

        auto *label = AppConfig.AddLabels();
        label->SetName(name);
        label->SetValue(value);
    }

    void InitMemLog(const NKikimrConfig::TMemoryLogConfig& mem) const {
        MemLogInit.Init(mem);
    }

    template <class TTag>
    void Option(const char* optname, TTag tag, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{ConfigUpdateTracer, ErrorCollector, ProtoConfigFileProvider};
        MutableConfigPart(refs, optname, tag, BaseConfig, AppConfig, ctx);
    }

    template <class TTag, class TContinuation>
    void Option(const char* optname, TTag tag, TContinuation continuation, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{ConfigUpdateTracer, ErrorCollector, ProtoConfigFileProvider};
        if (auto* res = MutableConfigPart(refs, optname, tag, BaseConfig, AppConfig, ctx)) {
            (this->*continuation)(*res);
        }
    }

    template <class TTag>
    void OptionMerge(const char* optname, TTag tag, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{ConfigUpdateTracer, ErrorCollector, ProtoConfigFileProvider};
        MutableConfigPartMerge(refs, optname, tag, AppConfig, ctx);
    }

    void PreFillLabels(const NConfig::TCommonAppOptions& cf) {
        Labels["node_id"] = ToString(cf.NodeId);
        Labels["node_host"] = Env.FQDNHostName();
        Labels["tenant"] = (cf.TenantName ? cf.TenantName.GetRef() : TString(""));
        Labels["node_type"] = (cf.NodeType ? cf.NodeType.GetRef() : TString(""));
        // will be replaced with proper version info
        Labels["branch"] = GetBranch();
        Labels["rev"] = GetProgramCommitId();
        Labels["dynamic"] = ToString(cf.NodeBrokerAddresses.empty() ? "false" : "true");

        for (const auto& [name, value] : Labels) {
            auto *label = AppConfig.AddLabels();
            label->SetName(name);
            label->SetValue(value);
        }
    }

    void SetupBootstrapConfigDefaults(NKikimrConfig::TBootstrap& bootstrapConfig) {
        CommonAppOptions.SetupBootstrapConfigDefaults(bootstrapConfig, ConfigUpdateTracer);
    };

    void SetupInterconnectConfigDefaults(NKikimrConfig::TInterconnectConfig& icConfig) {
        CommonAppOptions.SetupInterconnectConfigDefaults(icConfig, ConfigUpdateTracer);
    };

    void RegisterDynamicNode(NConfig::TCommonAppOptions& cf) {
        TVector<TString> addrs;

        cf.FillClusterEndpoints(AppConfig, addrs);

        if (!cf.InterconnectPort) {
            ythrow yexception() << "Either --node or --ic-port must be specified";
        }

        if (addrs.empty()) {
            ythrow yexception() << "List of Node Broker end-points is empty";
        }

        TString domainName = DeduceNodeDomain(cf, AppConfig);

        if (!cf.NodeHost) {
            cf.NodeHost = Env.FQDNHostName();
        }

        if (!cf.NodeResolveHost) {
            cf.NodeResolveHost = cf.NodeHost;
        }

        const TNodeRegistrationSettings settings {
            domainName,
            cf.NodeHost,
            cf.NodeAddress,
            cf.NodeResolveHost,
            cf.GetSchemePath(),
            cf.FixedNodeID,
            cf.InterconnectPort,
            cf.CreateNodeLocation(),
        };

        auto result = NodeBrokerClient.RegisterDynamicNode(cf.GrpcSslSettings, addrs, settings, Env);

        result->Apply(AppConfig, NodeId, ScopeId);
    }

    void ApplyConfigForNode(NKikimrConfig::TAppConfig &appConfig) {
        AppConfig.Swap(&appConfig);
        // Dynamic node config is defined by options and Node Broker response.
        AppConfig.MutableDynamicNodeConfig()->Swap(appConfig.MutableDynamicNodeConfig());
        // By now naming config should be loaded and probably replaced with
        // info from registration response. Don't lose it in case CMS has no
        // config for naming service.
        if (!AppConfig.HasNameserviceConfig()) {
            AppConfig.MutableNameserviceConfig()->Swap(appConfig.MutableNameserviceConfig());
            // FIXME(innokentii)
            // RunConfig.ConfigInitInfo[NKikimrConsole::TConfigItem::NameserviceConfigItem].Updates.pop_back();
        }
    }

    void InitStaticNode() {
        CommonAppOptions.ValidateStaticNodeConfig();

        Labels["dynamic"] = "false";
    }

    void InitDynamicNode() {
        Labels["dynamic"] = "true";
        RegisterDynamicNode(CommonAppOptions);

        Labels["node_id"] = ToString(NodeId);
        AddLabelToAppConfig("node_id", Labels["node_id"]);

        if (CommonAppOptions.IgnoreCmsConfigs) {
            return;
        }

        TVector<TString> addrs;
        CommonAppOptions.FillClusterEndpoints(AppConfig, addrs);

        TDynConfigSettings settings {
            NodeId,
            DeduceNodeDomain(CommonAppOptions, AppConfig),
            CommonAppOptions.TenantName.GetRef(),
            Env.FQDNHostName(),
            (CommonAppOptions.NodeType ? CommonAppOptions.NodeType.GetRef() : TString("")),
            AppConfig.GetAuthConfig().GetStaffApiUserToken(),
        };

        TMaybe<NKikimr::NClient::TConfigurationResult> result = DynConfigClient.GetConfig(CommonAppOptions.GrpcSslSettings, addrs, settings, Env);

        if (!result) {
            return;
        }

        NKikimrConfig::TAppConfig yamlConfig = GetYamlConfigFromResult(*result, Labels);
        NYamlConfig::ReplaceUnmanagedKinds(result->GetConfig(), yamlConfig);

        InitDebug.OldConfig.CopyFrom(result->GetConfig());
        InitDebug.YamlConfig.CopyFrom(yamlConfig);

        NKikimrConfig::TAppConfig appConfig = GetActualDynConfig(yamlConfig, result->GetConfig(), ConfigUpdateTracer);

        ApplyConfigForNode(appConfig);
    }

    void RegisterCliOptions(NLastGetopt::TOpts& opts) override {
        CommonAppOptions.RegisterCliOptions(opts);
        MbusAppOptions.RegisterCliOptions(opts);
        opts.AddLongOption("label", "labels for this node")
            .Optional().RequiredArgument("KEY=VALUE")
            .KVHandler([&](TString key, TString val) {
                Labels[key] = val;
            });

        opts.SetFreeArgDefaultTitle("PATH", "path to protobuf file; files are merged in order in which they are enlisted");
    }

    void Apply(
        NKikimrConfig::TAppConfig& appConfig,
        ui32& nodeId,
        TKikimrScopeId& scopeId,
        TString& tenantName,
        TBasicKikimrServicesMask& servicesMask,
        TMap<TString, TString>& labels,
        TString& clusterName,
        NKikimrConfig::TAppConfig& initialCmsConfig,
        NKikimrConfig::TAppConfig& initialCmsYamlConfig,
        THashMap<ui32, TConfigItemInfo>& configInitInfo) const override
    {
        appConfig = AppConfig;
        nodeId = NodeId;
        scopeId = ScopeId;
        tenantName = TenantName;
        servicesMask = ServicesMask;
        labels = Labels;
        clusterName = ClusterName;
        initialCmsConfig.CopyFrom(InitDebug.OldConfig);
        initialCmsYamlConfig.CopyFrom(InitDebug.YamlConfig);
        configInitInfo = InitDebug.ConfigTransformInfo;
    }
};

std::unique_ptr<IInitialConfigurator> MakeDefaultInitialConfigurator(
        NConfig::IErrorCollector& errorCollector,
        NConfig::IProtoConfigFileProvider& protoConfigFileProvider,
        NConfig::IConfigUpdateTracer& configUpdateTracer,
        NConfig::IMemLogInitializer& memLogInit,
        NConfig::INodeBrokerClient& nodeBrokerClient,
        NConfig::IDynConfigClient& dynConfigClient,
        NConfig::IEnv& env);

} // namespace NKikimr::NConfig
