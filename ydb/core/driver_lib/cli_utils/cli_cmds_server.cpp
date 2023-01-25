#include "cli.h"
#include "cli_cmds.h"
#include <ydb/core/base/location.h>
#include <ydb/core/base/path.h>
#include <ydb/core/driver_lib/run/run.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <util/digest/city.h>
#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>
#include <google/protobuf/text_format.h>

extern TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
extern TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();

namespace NKikimr {
namespace NDriverClient {

constexpr auto NODE_KIND_YDB = "ydb";
constexpr auto NODE_KIND_YQ = "yq";

class TClientCommandServerBase : public TClientCommand {
protected:
    NKikimrConfig::TAppConfig BaseConfig;
    NKikimrConfig::TAppConfig AppConfig;
    TKikimrRunConfig RunConfig;

    ui32 LogLevel; // log settings
    ui32 LogSamplingLevel; // log settings
    ui32 LogSamplingRate; // log settings
    TString LogFormat;// log settings
    TString SysLogServiceTag; //unique tags for sys logs
    TString LogFileName; // log file name to initialize file log backend
    TString ClusterName; // log settings

    ui32 NodeId;
    TString NodeIdValue;
    ui32 DefaultInterconnectPort = 19001;
    ui32 BusProxyPort;
    NBus::TBusQueueConfig ProxyBusQueueConfig;
    NBus::TBusServerSessionConfig ProxyBusSessionConfig;
    TVector<ui64> ProxyBindToProxy;
    ui32 MonitoringPort;
    TString MonitoringAddress;
    ui32 MonitoringThreads;
    TString MonitoringCertificateFile;
    TString RestartsCountFile;
    TString TracePath;
    size_t CompileInflightLimit; // MiniKQLCompileService
    TString UDFsDir;
    TVector<TString> UDFsPaths;
    TString TenantName;
    TVector<TString> NodeBrokerAddresses;
    ui32 NodeBrokerPort;
    bool NodeBrokerUseTls;
    bool FixedNodeID;
    bool IgnoreCmsConfigs;
    bool HierarchicalCfg;
    TString NodeAddress;
    TString NodeHost;
    TString NodeResolveHost;
    TString NodeDomain;
    ui32 InterconnectPort;
    ui32 SqsHttpPort;
    TString NodeKind = NODE_KIND_YDB;
    TString NodeType;
    TString DataCenter;
    TString Rack;
    ui32 Body;
    ui32 GRpcPort;
    ui32 GRpcsPort;
    TString GRpcPublicHost;
    ui32 GRpcPublicPort;
    ui32 GRpcsPublicPort;
    TVector<TString> GRpcPublicAddressesV4;
    TVector<TString> GRpcPublicAddressesV6;
    TString GRpcPublicTargetNameOverride;
    TString PathToGrpcCertFile;
    TString PathToInterconnectCertFile;
    TString PathToGrpcPrivateKeyFile;
    TString PathToInterconnectPrivateKeyFile;
    TString PathToGrpcCaFile;
    TString PathToInterconnectCaFile;
    TVector<TString> YamlConfigFiles;

    TClientCommandServerBase(const char *cmd, const char *description)
        : TClientCommand(cmd, {}, description)
        , RunConfig(AppConfig)
    {}

    virtual void Config(TConfig& config) override {
        TClientCommand::Config(config);

        LogLevel = NActors::NLog::PRI_WARN;
        LogSamplingLevel = NActors::NLog::PRI_DEBUG;
        LogSamplingRate = 0;

        NodeId = 0;
        NodeIdValue = "";
        BusProxyPort = NMsgBusProxy::TProtocol::DefaultPort;
        MonitoringPort = 0;
        MonitoringThreads = 10;
        RestartsCountFile = "";
        CompileInflightLimit = 100000;
        TenantName = "";
        NodeBrokerPort = 0;
        NodeBrokerUseTls = false;
        FixedNodeID = false;
        InterconnectPort = 0;
        SqsHttpPort = 0;
        IgnoreCmsConfigs = false;
        DataCenter = "";
        Rack = "";
        Body = 0;
        GRpcPort = 0;
        GRpcsPort = 0;
        GRpcPublicHost = "";
        GRpcPublicPort = 0;
        GRpcsPublicPort = 0;
        GRpcPublicAddressesV4.clear();
        GRpcPublicAddressesV6.clear();
        GRpcPublicTargetNameOverride = "";

        config.Opts->AddLongOption("cluster-name", "which cluster this node belongs to")
            .DefaultValue("unknown").OptionalArgument("STR").StoreResult(&ClusterName);
        config.Opts->AddLongOption("log-level", "default logging level").OptionalArgument("1-7")
            .DefaultValue(ToString(LogLevel)).StoreResult(&LogLevel);
        config.Opts->AddLongOption("log-sampling-level", "sample logs equal to or above this level").OptionalArgument("1-7")
            .DefaultValue(ToString(LogSamplingLevel)).StoreResult(&LogSamplingLevel);
        config.Opts->AddLongOption("log-sampling-rate",
                           "log only each Nth message with priority matching sampling level; 0 turns log sampling off")
            .OptionalArgument(Sprintf("0,%" PRIu32, Max<ui32>()))
            .DefaultValue(ToString(LogSamplingRate)).StoreResult(&LogSamplingRate);
        config.Opts->AddLongOption("log-format", "log format to use; short skips the priority and timestamp")
            .DefaultValue("full").OptionalArgument("full|short|json").StoreResult(&LogFormat);
        config.Opts->AddLongOption("syslog", "send to syslog instead of stderr").NoArgument();
        config.Opts->AddLongOption("syslog-service-tag", "unique tag for syslog").RequiredArgument("NAME").StoreResult(&SysLogServiceTag);
        config.Opts->AddLongOption("log-file-name", "file name for log backend").RequiredArgument("NAME").StoreResult(&LogFileName);
        config.Opts->AddLongOption("tcp", "start tcp interconnect").NoArgument();
        config.Opts->AddLongOption('n', "node", "Node ID or 'static' to auto-detect using naming file and ic-port.")
            .RequiredArgument("[NUM|static]").StoreResult(&NodeIdValue);
        config.Opts->AddLongOption("node-broker", "node broker address host:port")
                .RequiredArgument("ADDR").AppendTo(&NodeBrokerAddresses);
        config.Opts->AddLongOption("node-broker-port", "node broker port (hosts from naming file are used)")
                .RequiredArgument("PORT").StoreResult(&NodeBrokerPort);
        config.Opts->AddLongOption("node-broker-use-tls", "use tls for node broker (hosts from naming file are used)")
                .RequiredArgument("PORT").StoreResult(&NodeBrokerUseTls);
        config.Opts->AddLongOption("node-address", "address for dynamic node")
                .RequiredArgument("ADDR").StoreResult(&NodeAddress);
        config.Opts->AddLongOption("node-host", "hostname for dynamic node")
                .RequiredArgument("NAME").StoreResult(&NodeHost);
        config.Opts->AddLongOption("node-resolve-host", "resolve hostname for dynamic node")
                .RequiredArgument("NAME").StoreResult(&NodeResolveHost);
        config.Opts->AddLongOption("node-domain", "domain for dynamic node to register in")
                .RequiredArgument("NAME").StoreResult(&NodeDomain);
        config.Opts->AddLongOption("ic-port", "interconnect port")
                .RequiredArgument("NUM").StoreResult(&InterconnectPort);
        config.Opts->AddLongOption("sqs-port", "sqs port")
                .RequiredArgument("NUM").StoreResult(&SqsHttpPort);
        config.Opts->AddLongOption("proxy", "Bind to proxy(-ies)").RequiredArgument("ADDR").AppendTo(&ProxyBindToProxy);
        config.Opts->AddLongOption("tenant", "add binding for Local service to specified tenant, might be one of {'/<root>', '/<root>/<path_to_user>'}")
            .RequiredArgument("NAME").StoreResult(&TenantName);
        config.Opts->AddLongOption("mon-port", "Monitoring port").OptionalArgument("NUM").StoreResult(&MonitoringPort);
        config.Opts->AddLongOption("mon-address", "Monitoring address").OptionalArgument("ADDR").StoreResult(&MonitoringAddress);
        config.Opts->AddLongOption("mon-cert", "Monitoring certificate (https)").OptionalArgument("PATH").StoreResult(&MonitoringCertificateFile);
        config.Opts->AddLongOption("mon-threads", "Monitoring http server threads").RequiredArgument("NUM").StoreResult(&MonitoringThreads);
        config.Opts->AddLongOption("suppress-version-check", "Suppress version compatibility checking via IC").NoArgument();

        config.Opts->AddLongOption("sys-file", "actor system config file (use dummy config by default)").OptionalArgument("PATH");
        config.Opts->AddLongOption("naming-file", "static nameservice config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("domains-file", "domain config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("bs-file", "blobstorage config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("log-file", "log config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("ic-file", "interconnect config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("channels-file", "tablet channel profile config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("vdisk-file", "vdisk kind config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("drivemodel-file", "drive model config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("grpc-file", "gRPC config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("grpc-port", "enable gRPC server on port").RequiredArgument("PORT").StoreResult(&GRpcPort);
        config.Opts->AddLongOption("grpcs-port", "enable gRPC SSL server on port").RequiredArgument("PORT").StoreResult(&GRpcsPort);
        config.Opts->AddLongOption("grpc-public-host", "set public gRPC host for discovery").RequiredArgument("HOST").StoreResult(&GRpcPublicHost);
        config.Opts->AddLongOption("grpc-public-port", "set public gRPC port for discovery").RequiredArgument("PORT").StoreResult(&GRpcPublicPort);
        config.Opts->AddLongOption("grpcs-public-port", "set public gRPC SSL port for discovery").RequiredArgument("PORT").StoreResult(&GRpcsPublicPort);
        config.Opts->AddLongOption("grpc-public-address-v4", "set public ipv4 address for discovery").RequiredArgument("ADDR").EmplaceTo(&GRpcPublicAddressesV4);
        config.Opts->AddLongOption("grpc-public-address-v6", "set public ipv6 address for discovery").RequiredArgument("ADDR").EmplaceTo(&GRpcPublicAddressesV6);
        config.Opts->AddLongOption("grpc-public-target-name-override", "set public hostname override for TLS in discovery").RequiredArgument("HOST").StoreResult(&GRpcPublicTargetNameOverride);
        config.Opts->AddLongOption("kqp-file", "Kikimr Query Processor config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("incrhuge-file", "incremental huge blob keeper config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("memorylog-file", "set buffer size for memory log").OptionalArgument("PATH");
        config.Opts->AddLongOption("pq-file", "PersQueue config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("pqcd-file", "PersQueue cluster discovery config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("netclassifier-file", "NetClassifier config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("auth-file", "authorization configuration").OptionalArgument("PATH");
        config.Opts->AddLongOption("auth-token-file", "authorization token configuration").OptionalArgument("PATH");
        config.Opts->AddLongOption("key-file", "tanant encryption key configuration").OptionalArgument("PATH");
        config.Opts->AddLongOption("pdisk-key-file", "pdisk encryption key configuration").OptionalArgument("PATH");
        config.Opts->AddLongOption("sqs-file", "SQS config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("bootstrap-file", "Bootstrap config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("dyn-nodes-file", "Dynamic nodes config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("cms-file", "CMS config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("alloc-file", "Allocator config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("yql-file", "Yql Analytics config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("yq-file", "Yandex Query config file (deprecated)").OptionalArgument("PATH");
        config.Opts->AddLongOption("fq-file", "Federated Query config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("feature-flags-file", "File with feature flags to turn new features on/off").OptionalArgument("PATH");
        config.Opts->AddLongOption("rb-file", "File with resource broker customizations").OptionalArgument("PATH");
        config.Opts->AddLongOption("metering-file", "File with metering config").OptionalArgument("PATH");
        config.Opts->AddLongOption("audit-file", "File with audit config").OptionalArgument("PATH");
        config.Opts->AddLongOption('r', "restarts-count-file", "State for restarts monitoring counter,\nuse empty string to disable\n")
                .OptionalArgument("PATH").DefaultValue(RestartsCountFile).StoreResult(&RestartsCountFile);
        config.Opts->AddLongOption("compile-inflight-limit", "Limit on parallel programs compilation").OptionalArgument("NUM").StoreResult(&CompileInflightLimit);
        config.Opts->AddLongOption("udf", "Load shared library with UDF by given path").AppendTo(&UDFsPaths);
        config.Opts->AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory").StoreResult(&UDFsDir);
        config.Opts->AddLongOption("node-kind", Sprintf("Kind of the node (affects list of services activated allowed values are {'%s', '%s'} )", NODE_KIND_YDB, NODE_KIND_YQ))
                .RequiredArgument("NAME").StoreResult(&NodeKind);
        config.Opts->AddLongOption("node-type", "Type of the node")
                .RequiredArgument("NAME").StoreResult(&NodeType);
        config.Opts->AddLongOption("ignore-cms-configs", "Don't load configs from CMS")
                .NoArgument().SetFlag(&IgnoreCmsConfigs);
        config.Opts->AddLongOption("cert", "Path to client certificate file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCertFile);
        config.Opts->AddLongOption("grpc-cert", "Path to client certificate file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcCertFile);
        config.Opts->AddLongOption("ic-cert", "Path to client certificate file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCertFile);
        config.Opts->AddLongOption("key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        config.Opts->AddLongOption("grpc-key", "Path to private key file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcPrivateKeyFile);
        config.Opts->AddLongOption("ic-key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        config.Opts->AddLongOption("ca", "Path to certificate authority file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCaFile);
        config.Opts->AddLongOption("grpc-ca", "Path to certificate authority file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcCaFile);
        config.Opts->AddLongOption("ic-ca", "Path to certificate authority file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCaFile);
        config.Opts->AddLongOption("data-center", "data center name (used to describe dynamic node location)")
                .RequiredArgument("NAME").StoreResult(&DataCenter);
        config.Opts->AddLongOption("rack", "rack name (used to describe dynamic node location)")
                .RequiredArgument("NAME").StoreResult(&Rack);
        config.Opts->AddLongOption("body", "body name (used to describe dynamic node location)")
                .RequiredArgument("NUM").StoreResult(&Body);
        config.Opts->AddLongOption("yaml-config", "Yaml config").OptionalArgument("PATH").AppendTo(&YamlConfigFiles);
        config.Opts->AddLongOption("cms-config-cache-file", "Path to CMS cache config file").OptionalArgument("PATH")
            .StoreResult(&RunConfig.PathToConfigCacheFile);
        config.Opts->AddLongOption("http-proxy-file", "Http proxy config file").OptionalArgument("PATH");
        config.Opts->AddLongOption("public-http-file", "Public HTTP config file").OptionalArgument("PATH");

        config.Opts->AddHelpOption('h');

        // add messagebus proxy options
        config.Opts->AddLongOption("mbus", "Start MessageBus proxy").NoArgument();
        config.Opts->AddLongOption("mbus-port", "MessageBus proxy port").RequiredArgument("PORT").StoreResult(&BusProxyPort);
        config.Opts->AddLongOption("mbus-trace-path", "Path for trace files").RequiredArgument("PATH").StoreResult(&TracePath);
        SetMsgBusDefaults(ProxyBusSessionConfig, ProxyBusQueueConfig);
        ProxyBusSessionConfig.ConfigureLastGetopt(*config.Opts, "mbus-");
        ProxyBusQueueConfig.ConfigureLastGetopt(*config.Opts, "mbus-");

        config.Opts->AddLongOption("hierarchic-cfg", "Use hierarchical approach for configuration parts overriding")
        .NoArgument().SetFlag(&HierarchicalCfg);

        config.Opts->AddLongOption("label", "labels for this node")
            .Optional().RequiredArgument("KEY=VALUE")
            .KVHandler([&](TString key, TString val) {
                RunConfig.Labels[key] = val;
            });

        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("PATH", "path to protobuf file; files are merged in order in which they are enlisted");
    }

    template<typename TProto>
    TProto *MutableConfigPart(TConfig& config, const char *optname,
            bool (NKikimrConfig::TAppConfig::*hasConfig)() const,
            const TProto& (NKikimrConfig::TAppConfig::*getConfig)() const,
            TProto* (NKikimrConfig::TAppConfig::*mutableConfig)()) {
        TProto *res = nullptr;
        if (!HierarchicalCfg && (AppConfig.*hasConfig)()) {
            return nullptr; // this field is already provided in AppConfig, so we don't overwrite it
        }

        if (optname && config.ParseResult->Has(optname)) {
            const bool success = ParsePBFromFile(config.ParseResult->Get(optname), res = (AppConfig.*mutableConfig)());
            Y_VERIFY(success);
        } else if ((BaseConfig.*hasConfig)()) {
            res = (AppConfig.*mutableConfig)();
            res->CopyFrom((BaseConfig.*getConfig)());
        }

        return res;
    }

    template<typename TProto>
    TProto *MutableConfigPartMerge(TConfig& config, const char *optname,
            TProto* (NKikimrConfig::TAppConfig::*mutableConfig)()) {
        TProto *res = nullptr;

        if (config.ParseResult->Has(optname)) {
            TProto cfg;
            bool success = ParsePBFromFile(config.ParseResult->Get(optname), &cfg);
            Y_VERIFY(success);
            res = (AppConfig.*mutableConfig)();
            res->MergeFrom(cfg);
        }

        return res;
    }

    ui32 FindStaticNodeId() const {
        std::vector<TString> candidates = {HostName(), FQDNHostName()};
        for(auto& candidate: candidates) {
            candidate.to_lower();

            const NKikimrConfig::TStaticNameserviceConfig& nameserviceConfig = AppConfig.GetNameserviceConfig();
            for (const auto& node : nameserviceConfig.GetNode()) {
                if (node.GetHost() == candidate && InterconnectPort == node.GetPort()) {
                    return node.GetNodeId();
                }
            }
        }

        return 0;
    }

    virtual void Parse(TConfig& config) override {
        TClientCommand::Parse(config);

#define OPTION(NAME, FIELD) MutableConfigPart(config, NAME, &NKikimrConfig::TAppConfig::Has##FIELD, \
            &NKikimrConfig::TAppConfig::Get##FIELD, &NKikimrConfig::TAppConfig::Mutable##FIELD)
#define OPTION_MERGE(NAME, FIELD) MutableConfigPartMerge(config, NAME, &NKikimrConfig::TAppConfig::Mutable##FIELD)

        OPTION("auth-file", AuthConfig);
        LoadBaseConfig(config);
        LoadYamlConfig();
        OPTION_MERGE("auth-token-file", AuthConfig);

        // start memorylog as soon as possible
        if (auto mem = OPTION("memorylog-file", MemoryLogConfig)) {
            if (mem->HasLogBufferSize() && mem->GetLogBufferSize() > 0) {
                if (mem->HasLogGrainSize() && mem->GetLogGrainSize() > 0) {
                    TMemoryLog::CreateMemoryLogBuffer(mem->GetLogBufferSize(), mem->GetLogGrainSize());
                } else {
                    TMemoryLog::CreateMemoryLogBuffer(mem->GetLogBufferSize());
                }
                MemLogWriteNullTerm("Memory_log_has_been_started_YAHOO_");
            }
        }

        OPTION("naming-file", NameserviceConfig);

        if (config.ParseResult->Has("node")) {
            if (NodeIdValue == "static") {
                if (!AppConfig.HasNameserviceConfig() || !InterconnectPort)
                    ythrow yexception() << "'--node static' requires naming file and IC port to be specified";
                try {
                    NodeId = FindStaticNodeId();
                } catch(TSystemError& e) {
                    ythrow yexception() << "cannot detect host name: " << e.what();
                }
                if (!NodeId)
                    ythrow yexception() << "cannot detect node ID for " << HostName() << ":" << InterconnectPort
                        << " and for " << FQDNHostName() << ":" << InterconnectPort << Endl;
                Cout << "Determined node ID: " << NodeId << Endl;
            } else {
                if (!TryFromString(NodeIdValue, NodeId))
                    ythrow yexception() << "wrong '--node' value (should be NUM, 'static')";
            }
        }

        if (config.ParseResult->Has("tenant")) {
            if (!IsStartWithSlash(TenantName)) {
                ythrow yexception() << "lead / in --tenant parametr is always required.";
            }
            if (NodeId && NodeKind != NODE_KIND_YQ) {
                ythrow yexception() << "opt '--node' compatible only with '--tenant no', opt 'node' incompatible with any other values of opt '--tenant'";
            }
        }

        if (NodeKind == NODE_KIND_YDB) {
            // do nothing => default behaviour
        } else if (NodeKind == NODE_KIND_YQ) {
            RunConfig.ServicesMask.DisableAll();
            RunConfig.ServicesMask.EnableYQ();
        } else {
            ythrow yexception() << "wrong '--node-kind' value '" << NodeKind << "', only '" << NODE_KIND_YDB << "' or '" << NODE_KIND_YQ << "' is allowed";
        }

        MaybeRegisterAndLoadConfigs();

        LoadYamlConfig();

        OPTION("sys-file", ActorSystemConfig);
        if (!AppConfig.HasActorSystemConfig()) {
            AppConfig.MutableActorSystemConfig()->CopyFrom(*DummyActorSystemConfig());
        }

        OPTION("domains-file", DomainsConfig);
        OPTION("bs-file", BlobStorageConfig);

        if (auto logConfig = OPTION("log-file", LogConfig)) {
            if (config.ParseResult->Has("syslog"))
                logConfig->SetSysLog(true);
            if (config.ParseResult->Has("log-level"))
                logConfig->SetDefaultLevel(LogLevel);
            if (config.ParseResult->Has("log-sampling-level"))
                logConfig->SetDefaultSamplingLevel(LogSamplingLevel);
            if (config.ParseResult->Has("log-sampling-rate"))
                logConfig->SetDefaultSamplingRate(LogSamplingRate);
            if (config.ParseResult->Has("log-format"))
                logConfig->SetFormat(LogFormat);
            if (config.ParseResult->Has("cluster-name"))
                logConfig->SetClusterName(ClusterName);
        }
        // This flag is set per node and we prefer flag over CMS.
        if (config.ParseResult->Has("syslog-service-tag")
            && !AppConfig.GetLogConfig().GetSysLogService())
            AppConfig.MutableLogConfig()->SetSysLogService(SysLogServiceTag);

        if (config.ParseResult->Has("log-file-name"))
            AppConfig.MutableLogConfig()->SetBackendFileName(LogFileName);

        if (auto interconnectConfig = OPTION("ic-file", InterconnectConfig)) {
            if (config.ParseResult->Has("tcp")) {
                interconnectConfig->SetStartTcp(true);
            }
        }

        OPTION("channels-file", ChannelProfileConfig);

        if (auto bootstrapConfig = OPTION("bootstrap-file", BootstrapConfig)) {
            bootstrapConfig->MutableCompileServiceConfig()->SetInflightLimit(CompileInflightLimit);
        }

        OPTION("vdisk-file", VDiskConfig);
        OPTION("drivemodel-file", DriveModelConfig);
        OPTION("grpc-file", GRpcConfig);
        OPTION("dyn-nodes-file", DynamicNameserviceConfig);
        OPTION("cms-file", CmsConfig);
        OPTION("pq-file", PQConfig);
        OPTION("pqcd-file", PQClusterDiscoveryConfig);
        OPTION("netclassifier-file", NetClassifierConfig);
        OPTION("auth-file", AuthConfig);
        OPTION_MERGE("auth-token-file", AuthConfig);
        OPTION("key-file", KeyConfig);
        OPTION("pdisk-key-file", PDiskKeyConfig);
        OPTION("sqs-file", SqsConfig);
        OPTION("http-proxy-file", HttpProxyConfig);
        OPTION("public-http-file", PublicHttpConfig);
        OPTION("feature-flags-file", FeatureFlags);
        OPTION("rb-file", ResourceBrokerConfig);
        OPTION("metering-file", MeteringConfig);
        OPTION("audit-file", AuditConfig);
        OPTION("kqp-file", KQPConfig);
        OPTION("incrhuge-file", IncrHugeConfig);
        OPTION("alloc-file", AllocatorConfig);
        OPTION("yq-file", FederatedQueryConfig); // TODO: remove after migration (YQ-1467)
        OPTION("fq-file", FederatedQueryConfig);
        OPTION(nullptr, TracingConfig);
        OPTION(nullptr, FailureInjectionConfig);

        if (!AppConfig.HasAllocatorConfig()) {
            AppConfig.MutableAllocatorConfig()->CopyFrom(*DummyAllocatorConfig());
        }

        // apply certificates, if any
        if (!PathToInterconnectCertFile.Empty()) {
            AppConfig.MutableInterconnectConfig()->SetPathToCertificateFile(PathToInterconnectCertFile);
        }

        if (!PathToInterconnectPrivateKeyFile.Empty()) {
            AppConfig.MutableInterconnectConfig()->SetPathToPrivateKeyFile(PathToInterconnectPrivateKeyFile);
        }

        if (!PathToInterconnectCaFile.Empty()) {
            AppConfig.MutableInterconnectConfig()->SetPathToCaFile(PathToInterconnectCaFile);
        }

        if (AppConfig.HasGRpcConfig() && AppConfig.GetGRpcConfig().HasCert()) {
            AppConfig.MutableGRpcConfig()->SetPathToCertificateFile(AppConfig.GetGRpcConfig().GetCert());
        }

        if (!PathToGrpcCertFile.Empty()) {
            AppConfig.MutableGRpcConfig()->SetPathToCertificateFile(PathToGrpcCertFile);
        }

        if (AppConfig.HasGRpcConfig() && AppConfig.GetGRpcConfig().HasKey()) {
            AppConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(AppConfig.GetGRpcConfig().GetKey());
        }

        if (!PathToGrpcPrivateKeyFile.Empty()) {
            AppConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(PathToGrpcPrivateKeyFile);
        }

        if (AppConfig.HasGRpcConfig() && AppConfig.GetGRpcConfig().HasCA()) {
            AppConfig.MutableGRpcConfig()->SetPathToCaFile(AppConfig.GetGRpcConfig().GetCA());
        }

        if (!PathToGrpcCaFile.Empty()) {
            AppConfig.MutableGRpcConfig()->SetPathToCaFile(PathToGrpcCaFile);
        }

        if (!AppConfig.HasDomainsConfig())
            ythrow yexception() << "DomainsConfig is not provided";
        if (!AppConfig.HasChannelProfileConfig())
            ythrow yexception() << "ChannelProfileConfig is not provided";

        if (!config.ParseResult->Has("tenant") && RunConfig.ScopeId.IsEmpty()) {
            const TString myDomain = DeduceNodeDomain();
            for (const auto& domain : AppConfig.GetDomainsConfig().GetDomain()) {
                if (domain.GetName() == myDomain) {
                    RunConfig.ScopeId = TKikimrScopeId(0, domain.GetDomainId());
                    break;
                }
            }
        }
        if (NodeId)
            RunConfig.NodeId = NodeId;

        if (NodeKind == NODE_KIND_YQ && InterconnectPort) {
            auto& fqConfig = *AppConfig.MutableFederatedQueryConfig();
            auto& nmConfig = *fqConfig.MutableNodesManager();
            nmConfig.SetPort(InterconnectPort);
            nmConfig.SetHost(HostName());
        }

        if (config.ParseResult->Has("suppress-version-check")) {
            if (AppConfig.HasNameserviceConfig()) {
                AppConfig.MutableNameserviceConfig()->SetSuppressVersionCheck(true);
            } else {
                ythrow yexception() << "--suppress-version-check option is provided without static nameservice config";
            }
        }

        // apply options affecting UDF paths
        if (!AppConfig.HasUDFsDir())
            AppConfig.SetUDFsDir(UDFsDir);
        if (!AppConfig.UDFsPathsSize()) {
            for (const auto& path : UDFsPaths) {
                AppConfig.AddUDFsPaths(path);
            }
        }

        if (!AppConfig.HasMonitoringConfig())
            AppConfig.MutableMonitoringConfig()->SetMonitoringThreads(MonitoringThreads);
        if (!AppConfig.HasRestartsCountConfig() && RestartsCountFile)
            AppConfig.MutableRestartsCountConfig()->SetRestartsCountFile(RestartsCountFile);

        // Ports and node type are always applied (event if config was loaded from CMS).
        if (MonitoringPort)
            AppConfig.MutableMonitoringConfig()->SetMonitoringPort(MonitoringPort);
        if (MonitoringAddress)
            AppConfig.MutableMonitoringConfig()->SetMonitoringAddress(MonitoringAddress);
        if (MonitoringCertificateFile) {
            TString sslCertificate = TUnbufferedFileInput(MonitoringCertificateFile).ReadAll();
            if (!sslCertificate.empty()) {
                AppConfig.MutableMonitoringConfig()->SetMonitoringCertificate(sslCertificate);
            } else {
                ythrow yexception() << "invalid ssl certificate file";
            }
        }
        if (SqsHttpPort)
            RunConfig.AppConfig.MutableSqsConfig()->MutableHttpServerConfig()->SetPort(SqsHttpPort);
        if (GRpcPort) {
            auto& conf = *AppConfig.MutableGRpcConfig();
            conf.SetStartGRpcProxy(true);
            conf.SetPort(GRpcPort);
        }
        if (GRpcsPort) {
            auto& conf = *AppConfig.MutableGRpcConfig();
            conf.SetStartGRpcProxy(true);
            conf.SetSslPort(GRpcsPort);
        }
        if (GRpcPublicHost) {
            auto& conf = *AppConfig.MutableGRpcConfig();
            conf.SetPublicHost(GRpcPublicHost);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicHost()) {
                    ext.SetPublicHost(GRpcPublicHost);
                }
            }
        }
        if (GRpcPublicPort) {
            auto& conf = *AppConfig.MutableGRpcConfig();
            conf.SetPublicPort(GRpcPublicPort);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicPort()) {
                    ext.SetPublicPort(GRpcPublicPort);
                }
            }
        }
        if (GRpcsPublicPort) {
            auto& conf = *AppConfig.MutableGRpcConfig();
            conf.SetPublicSslPort(GRpcsPublicPort);
            for (auto& ext : *conf.MutableExtEndpoints()) {
                if (!ext.HasPublicSslPort()) {
                    ext.SetPublicSslPort(GRpcsPublicPort);
                }
            }
        }
        for (const auto& addr : GRpcPublicAddressesV4) {
            AppConfig.MutableGRpcConfig()->AddPublicAddressesV4(addr);
        }
        for (const auto& addr : GRpcPublicAddressesV6) {
            AppConfig.MutableGRpcConfig()->AddPublicAddressesV6(addr);
        }
        if (GRpcPublicTargetNameOverride) {
            AppConfig.MutableGRpcConfig()->SetPublicTargetNameOverride(GRpcPublicTargetNameOverride);
        }
        if (config.ParseResult->Has("node-type"))
            AppConfig.MutableTenantPoolConfig()->SetNodeType(NodeType);

        if (config.ParseResult->Has("tenant") && InterconnectPort != DefaultInterconnectPort) {
            AppConfig.MutableMonitoringConfig()->SetHostLabelOverride(HostAndICPort());
        }

        if (config.ParseResult->Has("data-center")) {
            AppConfig.MutableMonitoringConfig()->SetDataCenter(to_lower(DataCenter));
        }

        if (config.ParseResult->Has("tenant")) {
            auto &slot = *AppConfig.MutableTenantPoolConfig()->AddSlots();
            slot.SetId("static-slot");
            slot.SetTenantName(TenantName);
            slot.SetIsDynamic(false);
            RunConfig.TenantName = TenantName;
        } else {
            auto &slot = *AppConfig.MutableTenantPoolConfig()->AddSlots();
            slot.SetId("static-slot");
            slot.SetTenantName(CanonizePath(DeduceNodeDomain()));
            slot.SetIsDynamic(false);
            RunConfig.TenantName = CanonizePath(DeduceNodeDomain());
        }

        if (config.ParseResult->Has("data-center")) {
            if (AppConfig.HasFederatedQueryConfig()) {
                AppConfig.MutableFederatedQueryConfig()->MutableNodesManager()->SetDataCenter(to_lower(DataCenter));
            }
        }

        // MessageBus options.

        if (!AppConfig.HasMessageBusConfig()) {
            auto messageBusConfig = AppConfig.MutableMessageBusConfig();
            messageBusConfig->SetStartBusProxy(config.ParseResult->Has(config.Opts->FindLongOption("mbus")));
            messageBusConfig->SetBusProxyPort(BusProxyPort);

            if (!messageBusConfig->GetStartBusProxy()) {
                for (const auto &option : config.Opts->Opts_) {
                    for (const TString &longName : option->GetLongNames()) {
                        if (longName.StartsWith("mbus-") && config.ParseResult->Has(option.Get())) {
                            ythrow yexception() << "option --" << longName << " is useless without --mbus option";
                        }
                    }
                }
            }

            auto queueConfig = messageBusConfig->MutableProxyBusQueueConfig();
            queueConfig->SetName(ProxyBusQueueConfig.Name);
            queueConfig->SetNumWorkers(ProxyBusQueueConfig.NumWorkers);

            auto sessionConfig = messageBusConfig->MutableProxyBusSessionConfig();

            // TODO use macro from messagebus header file
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

        RunConfig.Labels["node_id"] = ToString(NodeId);
        RunConfig.Labels["node_host"] = FQDNHostName();
        RunConfig.Labels["tenant"] = RunConfig.TenantName;
        // will be replaced with proper version info
        RunConfig.Labels["branch"] = GetBranch();
        RunConfig.Labels["rev"] = ToString(GetProgramSvnRevision());
    }

    inline bool LoadConfigFromCMS() {
        TVector<TString> addrs;
        FillClusterEndpoints(addrs);

        SetRandomSeed(TInstant::Now().MicroSeconds());

        int minAttempts = 10;
        int attempts = 0;

        TString error;

        while (attempts < minAttempts) {
            for (const auto &addr : addrs) {
                // Randomized backoff
                if (attempts > 0)
                    Sleep(TDuration::MilliSeconds(500 + RandomNumber<ui64>(1000)));

                NClient::TKikimr kikimr(GetKikimr(addr));
                auto configurator = kikimr.GetNodeConfigurator();

                Cout << "Trying to get configs from " << addr << Endl;

                auto result = configurator.SyncGetNodeConfig(RunConfig.NodeId,
                                                             FQDNHostName(),
                                                             TenantName,
                                                             NodeType,
                                                             DeduceNodeDomain(),
                                                             AppConfig.GetAuthConfig().GetStaffApiUserToken());

                if (result.IsSuccess()) {
                    auto appConfig = result.GetConfig();

                    if (RunConfig.PathToConfigCacheFile) {
                        Cout << "Saving config to cache file " << RunConfig.PathToConfigCacheFile << Endl;
                        if (!SaveConfigForNodeToCache(appConfig)) {
                            Cout << "Failed to save config to cache file" << Endl;
                        }
                    }

                    BaseConfig.Swap(&appConfig);

                    Cout << "Success." << Endl;

                    return true;
                }

                error = result.GetErrorMessage();
                Cerr << "Configuration error: " << error << Endl;
                ++attempts;
            }
        }

        return false;
    }

    inline bool LoadConfigFromCache() {
        if (RunConfig.PathToConfigCacheFile) {
            NKikimrConfig::TAppConfig config;
            if (GetCachedConfig(config)) {
                BaseConfig.Swap(&config);

                return true;
            }
        }

        return false;
    }

    inline void LoadYamlConfig() {
        for(const TString& yamlConfigFile: YamlConfigFiles) {
            auto yamlConfig = TFileInput(yamlConfigFile);
            NKikimrConfig::TAppConfig parsedConfig;
            NKikimr::NYaml::Parse(yamlConfig.ReadAll(), parsedConfig);
            const google::protobuf::Descriptor* descriptor = AppConfig.GetDescriptor();
            const google::protobuf::Reflection* reflection = AppConfig.GetReflection();
            for(int fieldIdx = 0; fieldIdx < descriptor->field_count(); ++fieldIdx) {
                const google::protobuf::FieldDescriptor* fieldDescriptor = descriptor->field(fieldIdx);
                if (!fieldDescriptor)
                    continue;

                if (fieldDescriptor->is_repeated()) {
                    continue;
                }

                if (reflection->HasField(AppConfig, fieldDescriptor)) {
                    // field is already set in app config
                    continue;
                }

                if (reflection->HasField(parsedConfig, fieldDescriptor)) {
                    reflection->SwapFields(&AppConfig, &parsedConfig, {fieldDescriptor});
                }
            }
        }
        // TODO: remove after migration (YQ-1467)
        if (AppConfig.HasYandexQueryConfig()) {
            AppConfig.MutableFederatedQueryConfig()->MergeFrom(AppConfig.GetYandexQueryConfig());
        }
    }

    inline bool LoadBootstrapConfig(TConfig& config) {
        bool res = false;
        for (const TString& path : config.ParseResult->GetFreeArgs()) {
            NKikimrConfig::TAppConfig parsedConfig;
            const bool result = ParsePBFromFile(path, &parsedConfig);
            Y_VERIFY(result);
            BaseConfig.MergeFrom(parsedConfig);
            res = true;
        }

        return res;
    }

    void LoadBaseConfig(TConfig& config) {
        if (HierarchicalCfg) {
            if (LoadConfigFromCMS())
                return;
            if (LoadConfigFromCache())
                return;
            if (LoadBootstrapConfig(config))
                return;

            ythrow yexception() << "cannot load configuration";
        } else {
            LoadBootstrapConfig(config);
        }
    }

    TString DeduceNodeDomain() {
        if (NodeDomain)
            return NodeDomain;
        if (AppConfig.GetDomainsConfig().DomainSize() == 1)
            return AppConfig.GetDomainsConfig().GetDomain(0).GetName();
        if (AppConfig.GetTenantPoolConfig().SlotsSize() == 1) {
            auto &slot = AppConfig.GetTenantPoolConfig().GetSlots(0);
            if (slot.GetDomainName())
                return slot.GetDomainName();
            auto &tenantName = slot.GetTenantName();
            if (IsStartWithSlash(tenantName))
                return ToString(ExtractDomain(tenantName));
        }
        return "";
    }

    bool GetCachedConfig(NKikimrConfig::TAppConfig &appConfig) {
        Y_VERIFY_DEBUG(RunConfig.PathToConfigCacheFile, "GetCachedConfig called with a cms config cache file set");

        try {
            auto cacheFile = TFileInput(RunConfig.PathToConfigCacheFile);
            if (!google::protobuf::TextFormat::ParseFromString(cacheFile.ReadAll(), &appConfig))
                ythrow yexception() << "Failed to parse config protobuf from string";
            return true;
        } catch (const yexception &ex) {
            Cerr << "WARNING: an exception occurred while getting config from cache file: " << ex.what() << Endl;
        }
        return false;
    }

    void LoadCachedConfigsForStaticNode() {
        NKikimrConfig::TAppConfig appConfig;

        // log config
        if (GetCachedConfig(appConfig) && appConfig.HasLogConfig()) {
            AppConfig.MutableLogConfig()->CopyFrom(appConfig.GetLogConfig());
        }
    }

    void MaybeRegisterAndLoadConfigs()
    {
        // static node
        if (NodeBrokerAddresses.empty() && !NodeBrokerPort) {
            if (!NodeId) {
                ythrow yexception() << "Either --node [NUM|'static'] or --node-broker[-port] should be specified";
            }

            if (!HierarchicalCfg && RunConfig.PathToConfigCacheFile)
                LoadCachedConfigsForStaticNode();
            return;
        }

        RegisterDynamicNode();
        if (!HierarchicalCfg && !IgnoreCmsConfigs)
            LoadConfigForDynamicNode();
    }

    THolder<NClient::TRegistrationResult> TryToRegisterDynamicNode(
            const TString &addr,
            const TString &domainName,
            const TString &nodeHost,
            const TString &nodeAddress,
            const TString &nodeResolveHost,
        const TMaybe<TString>& path) {
        NClient::TKikimr kikimr(GetKikimr(addr));
        auto registrant = kikimr.GetNodeRegistrant();

        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter(DataCenter);
        location.SetRack(Rack);
        location.SetUnit(ToString(Body));
        TNodeLocation loc(location);

        NActorsInterconnect::TNodeLocation legacy;
        legacy.SetDataCenterNum(DataCenterFromString(DataCenter));
        legacy.SetRoomNum(0);
        legacy.SetRackNum(RackFromString(Rack));
        legacy.SetBodyNum(Body);
        loc.InheritLegacyValue(TNodeLocation(legacy));

        Cout << "Trying to register at " << addr << Endl;

        return MakeHolder<NClient::TRegistrationResult>
            (registrant.SyncRegisterNode(ToString(domainName),
                                         nodeHost,
                                         InterconnectPort,
                                         nodeAddress,
                                         nodeResolveHost,
                                         std::move(loc),
                                         FixedNodeID,
                                         path));
    }

    void FillClusterEndpoints(TVector<TString> &addrs) {
        if (!NodeBrokerAddresses.empty()) {
            for (auto addr: NodeBrokerAddresses) {
                addrs.push_back(addr);
            }
        } else {
            Y_VERIFY(NodeBrokerPort);
            for (auto &node : RunConfig.AppConfig.MutableNameserviceConfig()->GetNode()) {
                addrs.emplace_back(TStringBuilder() << (NodeBrokerUseTls ? "grpcs://" : "") << node.GetHost() << ':' << NodeBrokerPort);
            }
        }
        ShuffleRange(addrs);
    }

    TString HostAndICPort() {
        try {
            auto hostname = to_lower(HostName());
            hostname = hostname.substr(0, hostname.find('.'));
            return TStringBuilder() << hostname << ":" << InterconnectPort;
        } catch (TSystemError& error) {
            return "";
        }
    }

    TMaybe<TString> GetSchemePath() {
        if (TenantName.StartsWith('/')) {
            return TenantName; // TODO(alexvru): fix it
        }
        return {};
    }

    void RegisterDynamicNode() {
        TVector<TString> addrs;
        auto &dnConfig = *RunConfig.AppConfig.MutableDynamicNodeConfig();

        FillClusterEndpoints(addrs);

        if (!InterconnectPort)
            ythrow yexception() << "Either --node or --ic-port should be specified";

        if (addrs.empty()) {
            ythrow yexception() << "List of Node Broker end-points is empty";
        }

        TString domainName = DeduceNodeDomain();
        if (!NodeHost)
            NodeHost = FQDNHostName();
        if (!NodeResolveHost)
            NodeResolveHost = NodeHost;

        THolder<NClient::TRegistrationResult> result;
        while (!result || !result->IsSuccess()) {
            for (auto addr : addrs) {
                result = TryToRegisterDynamicNode(addr, domainName, NodeHost, NodeAddress, NodeResolveHost, GetSchemePath());
                if (result->IsSuccess()) {
                    Cout << "Success. Registered as " << result->GetNodeId() << Endl;
                    break;
                }
                Cerr << "Registration error: " << result->GetErrorMessage() << Endl;
            }
            if (!result || !result->IsSuccess())
                Sleep(TDuration::Seconds(1));
        }
        Y_VERIFY(result);

        if (!result->IsSuccess())
            ythrow yexception() << "Cannot register dynamic node: " << result->GetErrorMessage();

        RunConfig.NodeId = result->GetNodeId();
        RunConfig.ScopeId = TKikimrScopeId(result->GetScopeId());
        auto &nsConfig = *RunConfig.AppConfig.MutableNameserviceConfig();

        nsConfig.ClearNode();

        for (auto &node : result->Record().GetNodes()) {
            if (node.GetNodeId() == result->GetNodeId()) {
                dnConfig.MutableNodeInfo()->CopyFrom(node);
            } else {
                auto &info = *nsConfig.AddNode();
                info.SetNodeId(node.GetNodeId());
                info.SetAddress(node.GetAddress());
                info.SetPort(node.GetPort());
                info.SetHost(node.GetHost());
                info.SetInterconnectHost(node.GetResolveHost());
                info.MutableLocation()->CopyFrom(node.GetLocation());
            }
        }
    }

    void ApplyConfigForNode(NKikimrConfig::TAppConfig &appConfig) {
        AppConfig.Swap(&appConfig);
        // Dynamic node config is defined by options and Node Broker response.
        AppConfig.MutableDynamicNodeConfig()->Swap(appConfig.MutableDynamicNodeConfig());
        // By now naming config should be loaded and probably replaced with
        // info from registration response. Don't lose it in case CMS has no
        // config for naming service.
        if (!AppConfig.HasNameserviceConfig())
            AppConfig.MutableNameserviceConfig()->Swap(appConfig.MutableNameserviceConfig());
    }

    bool SaveConfigForNodeToCache(const NKikimrConfig::TAppConfig &appConfig) {
        Y_VERIFY_DEBUG(RunConfig.PathToConfigCacheFile, "SaveConfigForNodeToCache called without a cms config cache file set");

        // Ensure "atomicity" by writing to temp file and renaming it
        const TString pathToTempFile = RunConfig.PathToConfigCacheFile + ".tmp";
        TString proto;
        bool status;
        try {
            TFileOutput tempFile(pathToTempFile);
            status = google::protobuf::TextFormat::PrintToString(appConfig, &proto);
            if (status) {
                tempFile << proto;
                if (!NFs::Rename(pathToTempFile, RunConfig.PathToConfigCacheFile)) {
                    ythrow yexception() << "Failed to rename temporary file " << LastSystemError() << " " << LastSystemErrorText();
                }
            }
        } catch (const yexception& ex) {
            Cerr << "WARNING: an exception occured while saving config to cache file: " << ex.what() << Endl;
            status = false;
        }

        return status;
    }

    bool TryToLoadConfigForDynamicNodeFromCMS(const TString &addr, TString &error) {
        NClient::TKikimr kikimr(GetKikimr(addr));
        auto configurator = kikimr.GetNodeConfigurator();

        Cout << "Trying to get configs from " << addr << Endl;

        auto result = configurator.SyncGetNodeConfig(RunConfig.NodeId,
                                                     FQDNHostName(),
                                                     TenantName,
                                                     NodeType,
                                                     DeduceNodeDomain(),
                                                     AppConfig.GetAuthConfig().GetStaffApiUserToken());

        if (!result.IsSuccess()) {
            error = result.GetErrorMessage();
            Cerr << "Configuration error: " << error << Endl;
            return false;
        }

        Cout << "Success." << Endl;

        auto appConfig = result.GetConfig();

        if (RunConfig.PathToConfigCacheFile) {
            Cout << "Saving config to cache file " << RunConfig.PathToConfigCacheFile << Endl;
            if (!SaveConfigForNodeToCache(appConfig)) {
                Cout << "Failed to save config to cache file" << Endl;
            }
        }

        ApplyConfigForNode(appConfig);

        return true;
    }

    bool LoadConfigForDynamicNodeFromCache() {
        NKikimrConfig::TAppConfig config;
        if (GetCachedConfig(config)) {
            ApplyConfigForNode(config);
            return true;
        }
        return false;
    }

    void LoadConfigForDynamicNode() {
        auto res = false;
        TString error;
        TVector<TString> addrs;

        FillClusterEndpoints(addrs);

        SetRandomSeed(TInstant::Now().MicroSeconds());
        int minAttempts = 10;
        int attempts = 0;
        while (!res && attempts < minAttempts) {
            for (auto addr : addrs) {
                res = TryToLoadConfigForDynamicNodeFromCMS(addr, error);
                ++attempts;
                if (res)
                    break;
            }
            // Randomized backoff
            if (!res)
                Sleep(TDuration::MilliSeconds(500 + RandomNumber<ui64>(1000)));
        }

        if (!res) {
            Cerr << "WARNING: couldn't load config from CMS: " << error << Endl;
            if (RunConfig.PathToConfigCacheFile) {
                Cout << "Loading config from cache file " << RunConfig.PathToConfigCacheFile << Endl;
                if (!LoadConfigForDynamicNodeFromCache())
                    Cerr << "WARNING: couldn't load config from cache file" << Endl;
            } else {
                Cerr << "WARNING: option --cms-config-cache-file was not set, ";
                Cerr << "couldn't load config from cache file" << Endl;
            }
        }
    }

private:
    NClient::TKikimr GetKikimr(const TString& addr) {
        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
        NGrpc::TGRpcClientConfig grpcConfig(endpoint.Address, TDuration::Seconds(5));
        grpcConfig.LoadBalancingPolicy = "round_robin";
        if (endpoint.EnableSsl.Defined()) {
            grpcConfig.EnableSsl = endpoint.EnableSsl.GetRef();
            auto& sslCredentials = grpcConfig.SslCredentials;
            if (PathToGrpcCaFile) {
                sslCredentials.pem_root_certs = ReadFromFile(PathToGrpcCaFile, "CA certificates");
            }
            if (PathToGrpcCertFile && PathToGrpcPrivateKeyFile) {
                sslCredentials.pem_cert_chain = ReadFromFile(PathToGrpcCertFile, "Client certificates");
                sslCredentials.pem_private_key = ReadFromFile(PathToGrpcPrivateKeyFile, "Client certificates key");
            }
        }
        return NClient::TKikimr(grpcConfig);
    }
};

class TClientCommandServerConfig : public TClientCommandServerBase {
public:
    TClientCommandServerConfig()
        : TClientCommandServerBase("serverconfig", "Generate configs for new-style invocation of server")
    {
    }

    virtual void Config(TConfig& config) override {
        TClientCommandServerBase::Config(config);
        config.Opts->AddLongOption("dump-config-to", "Dump final application config protobuf to PATH and terminate").RequiredArgument("PATH").Required();
    }

    virtual int Run(TConfig& config) override {
        Y_VERIFY(config.ParseResult->Has("dump-config-to"));

        TString proto;
        const bool status = google::protobuf::TextFormat::PrintToString(AppConfig, &proto);
        Y_VERIFY(status);
        TString path = config.ParseResult->Get("dump-config-to");
        TFileOutput file(path);
        file << proto;

        return 0;
    }
};

class TClientCommandServer : public TClientCommandServerBase {
public:
    TClientCommandServer(std::shared_ptr<TModuleFactories> factories)
        : TClientCommandServerBase("server", "Execute YDB server")
        , Factories(std::move(factories))
    {}

    virtual int Run(TConfig &/*config*/) override {
        Y_VERIFY(RunConfig.NodeId);
        return MainRun(RunConfig, Factories);
    }

private:
    std::shared_ptr<TModuleFactories> Factories;
};

void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories) {
    parent.AddCommand(std::make_unique<TClientCommandServer>(factories));
    parent.AddCommand(std::make_unique<TClientCommandServerConfig>());
}

}
}
