// FIXME, move below
#include <ydb/core/config/init/init.h>

#include "cli.h"
#include "cli_cmds.h"
#include <ydb/core/base/location.h>
#include <ydb/core/base/path.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/core/driver_lib/run/run.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <util/digest/city.h>
#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/generic/ptr.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>
#include <google/protobuf/text_format.h>
#include <ydb/core/protos/alloc.pb.h>
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/compile_service_config.pb.h>
#include <ydb/core/protos/http_config.pb.h>
#include <ydb/core/protos/tenant_pool.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/protos/resource_broker.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

extern TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
extern TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();

namespace NKikimr::NDriverClient {

class TClientCommandServer : public TClientCommand {
public:
    TClientCommandServer(std::shared_ptr<TModuleFactories> factories)
        : TClientCommand("server", {}, "Execute YDB server")
        , Factories(std::move(factories))
    {
        ErrorCollector = std::make_unique<NConfig::TDefaultErrorCollector>();
        ProtoConfigFileProvider = std::make_unique<NConfig::TDefaultProtoConfigFileProvider>();
        ConfigUpdateTracer = std::make_unique<NConfig::TDefaultConfigUpdateTracer>();
        Env = std::make_unique<NConfig::TDefaultEnv>();
    }

    int Run(TConfig &/*config*/) override {

        TKikimrRunConfig RunConfig(AppConfig);
        // FIXME: fill
        Y_ABORT_UNLESS(RunConfig.NodeId);
        return MainRun(RunConfig, Factories);
    }
protected:
    std::shared_ptr<TModuleFactories> Factories;

    ui32 NodeId = 0;
    TBasicKikimrServicesMask ServicesMask;
    TKikimrScopeId ScopeId;
    TString TenantName;
    TString ClusterName;

    TMap<TString, TString> Labels;

    struct TAppInitDebugInfo {
        NKikimrConfig::TAppConfig OldConfig;
        NKikimrConfig::TAppConfig YamlConfig;
        THashMap<ui32, TConfigItemInfo> ConfigTransformInfo;
    };

    TAppInitDebugInfo InitDebug;

    NKikimrConfig::TAppConfig BaseConfig;
    NKikimrConfig::TAppConfig AppConfig;

    NConfig::TConfigFields ConfigFields;
    NConfig::TMbusConfigFields MbusConfigFields;

    std::unique_ptr<NConfig::IErrorCollector> ErrorCollector;
    std::unique_ptr<NConfig::IProtoConfigFileProvider> ProtoConfigFileProvider;
    std::unique_ptr<NConfig::IConfigUpdateTracer> ConfigUpdateTracer;
    std::unique_ptr<NConfig::IEnv> Env;

    void Config(TConfig& config) override {
        TClientCommand::Config(config);

        NConfig::AddProtoConfigOptions(*ProtoConfigFileProvider);
        ConfigFields.RegisterCliOptions(*config.Opts);
        MbusConfigFields.RegisterCliOptions(*config.Opts);
        ProtoConfigFileProvider->RegisterCliOptions(*config.Opts);

        config.Opts->AddHelpOption('h');

        config.Opts->AddLongOption("label", "labels for this node")
            .Optional().RequiredArgument("KEY=VALUE")
            .KVHandler([&](TString key, TString val) {
                Labels[key] = val;
            });

        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("PATH", "path to protobuf file; files are merged in order in which they are enlisted");
    }

    void InitStaticNode() {
        ConfigFields.ValidateStaticNodeConfig();

        Labels["dynamic"] = "false";
    }

    static ui32 NextValidKind(ui32 kind) {
        do {
            ++kind;
            if (kind != NKikimrConsole::TConfigItem::Auto && NKikimrConsole::TConfigItem::EKind_IsValid(kind)) {
                break;
            }
        } while (kind <= NKikimrConsole::TConfigItem::EKind_MAX);
        return kind;
    }

    static bool HasCorrespondingManagedKind(ui32 kind, const NKikimrConfig::TAppConfig& appConfig) {
        return (kind == NKikimrConsole::TConfigItem::NameserviceConfigItem && appConfig.HasNameserviceConfig()) ||
               (kind == NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem && appConfig.HasNetClassifierDistributableConfig()) ||
               (kind == NKikimrConsole::TConfigItem::NamedConfigsItem && appConfig.NamedConfigsSize());
    }

    NKikimrConfig::TAppConfig GetYamlConfigFromResult(const NKikimr::NClient::TConfigurationResult& result, const TMap<TString, TString>& labels) const {
        NKikimrConfig::TAppConfig yamlConfig;
        if (result.HasYamlConfig() && !result.GetYamlConfig().empty()) {
            NYamlConfig::ResolveAndParseYamlConfig(
                result.GetYamlConfig(),
                result.GetVolatileYamlConfigs(),
                labels,
                yamlConfig);
        }
        return yamlConfig;
    }

    NKikimrConfig::TAppConfig GetActualDynConfig(const NKikimrConfig::TAppConfig& yamlConfig, const NKikimrConfig::TAppConfig& regularConfig) const {
        if (yamlConfig.GetYamlConfigEnabled()) {
            for (ui32 kind = NKikimrConsole::TConfigItem::EKind_MIN; kind <= NKikimrConsole::TConfigItem::EKind_MAX; NextValidKind(kind)) {
                if (HasCorrespondingManagedKind(kind, yamlConfig)) {
                    TRACE_CONFIG_CHANGE_INPLACE(kind, ReplaceConfigWithConsoleProto);
                } else {
                    TRACE_CONFIG_CHANGE_INPLACE(kind, ReplaceConfigWithConsoleYaml);
                }
            }

            return yamlConfig;
        }

        for (ui32 kind = NKikimrConsole::TConfigItem::EKind_MIN; kind <= NKikimrConsole::TConfigItem::EKind_MAX; NextValidKind(kind)) {
            TRACE_CONFIG_CHANGE_INPLACE(kind, ReplaceConfigWithConsoleProto);
        }

        return regularConfig;
    }

    void InitDynamicNode() {
        Labels["dynamic"] = "true";
        RegisterDynamicNode(ConfigFields);

        Labels["node_id"] = ToString(NodeId);
        AddLabelToAppConfig("node_id", Labels["node_id"]);

        if (ConfigFields.IgnoreCmsConfigs) {
            return;
        }

        TMaybe<NKikimr::NClient::TConfigurationResult> result;
        LoadConfigForDynamicNode(ConfigFields, result);

        if (!result) {
            return;
        }

        NKikimrConfig::TAppConfig yamlConfig = GetYamlConfigFromResult(*result, Labels);
        NYamlConfig::ReplaceUnmanagedKinds(result->GetConfig(), yamlConfig);

        InitDebug.OldConfig.CopyFrom(result->GetConfig());
        InitDebug.YamlConfig.CopyFrom(yamlConfig);

        NKikimrConfig::TAppConfig appConfig = GetActualDynConfig(yamlConfig, result->GetConfig());

        ApplyConfigForNode(appConfig);
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        MbusConfigFields.ValidateCliOptions(*config.Opts, *config.ParseResult);
        ParseImpl(config.ParseResult->GetFreeArgs());
    }

    // =========================================

    void ParseImpl(const TVector<TString>& freeArgs) {
        using TCfg = NKikimrConfig::TAppConfig;

        NConfig::TConfigRefs refs{*ConfigUpdateTracer, *ErrorCollector, *ProtoConfigFileProvider};

        Option("auth-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());
        LoadBootstrapConfig(*ProtoConfigFileProvider, *ErrorCollector, freeArgs, BaseConfig);
        LoadYamlConfig(refs, ConfigFields.YamlConfigFile, AppConfig, CALL_CTX());
        OptionMerge("auth-token-file", TCfg::TAuthConfigFieldTag{}, CALL_CTX());

        // start memorylog as soon as possible
        Option("memorylog-file", TCfg::TMemoryLogConfigFieldTag{}, &TClientCommandServer::InitMemLog, CALL_CTX());
        Option("naming-file", TCfg::TNameserviceConfigFieldTag{}, CALL_CTX());

        ConfigFields.NodeId = ConfigFields.DeduceNodeId(AppConfig, *Env);
        Cout << "Determined node ID: " << ConfigFields.NodeId << Endl;

        ConfigFields.ValidateTenant();

        ConfigFields.ApplyServicesMask(ServicesMask);

        PreFillLabels(ConfigFields);

        if (ConfigFields.IsStaticNode()) {
            InitStaticNode();
        } else {
            InitDynamicNode();
        }

        LoadYamlConfig(refs, ConfigFields.YamlConfigFile, AppConfig, CALL_CTX());

        Option("sys-file", TCfg::TActorSystemConfigFieldTag{}, CALL_CTX());

        if (!AppConfig.HasActorSystemConfig()) {
            AppConfig.MutableActorSystemConfig()->CopyFrom(*DummyActorSystemConfig());
            TRACE_CONFIG_CHANGE_INPLACE_T(ActorSystemConfig, SetExplicitly);
        }

        Option("domains-file", TCfg::TDomainsConfigFieldTag{}, CALL_CTX());
        Option("bs-file", TCfg::TBlobStorageConfigFieldTag{}, CALL_CTX());
        Option("log-file", TCfg::TLogConfigFieldTag{}, &TClientCommandServer::SetupLogConfigDefaults, CALL_CTX());

        // This flag is set per node and we prefer flag over CMS.
        ConfigFields.ApplyLogSettings(AppConfig, ConfigUpdateTracer.get());

        Option("ic-file", TCfg::TInterconnectConfigFieldTag{}, &TClientCommandServer::SetupInterconnectConfigDefaults, CALL_CTX());
        Option("channels-file", TCfg::TChannelProfileConfigFieldTag{}, CALL_CTX());
        Option("bootstrap-file", TCfg::TBootstrapConfigFieldTag{}, &TClientCommandServer::SetupBootstrapConfigDefaults, CALL_CTX());
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

        ConfigFields.ApplyFields(AppConfig, *Env, ConfigUpdateTracer.get());

       // MessageBus options.
        if (!AppConfig.HasMessageBusConfig()) {
            MbusConfigFields.InitMessageBusConfig(AppConfig);
            TRACE_CONFIG_CHANGE_INPLACE_T(MessageBusConfig, UpdateExplicitly);
        }

        TenantName = FillTenantPoolConfig(ConfigFields);

        FillData(ConfigFields);
    }

    void FillData(const NConfig::TConfigFields& cf) {
        if (cf.TenantName && ScopeId.IsEmpty()) {
            const TString myDomain = DeduceNodeDomain(cf);
            for (const auto& domain : AppConfig.GetDomainsConfig().GetDomain()) {
                if (domain.GetName() == myDomain) {
                    ScopeId = TKikimrScopeId(0, domain.GetDomainId());
                    break;
                }
            }
        }

        if (cf.NodeId) { // FIXME: do we really need it ???
            NodeId = cf.NodeId;

            Labels["node_id"] = ToString(NodeId);
            AddLabelToAppConfig("node_id", Labels["node_id"]);
        }

        InitDebug.ConfigTransformInfo = ConfigUpdateTracer->Dump();
        ClusterName = AppConfig.GetNameserviceConfig().GetClusterUUID();
    }

    TString FillTenantPoolConfig(const NConfig::TConfigFields& cf) {
        auto &slot = *AppConfig.MutableTenantPoolConfig()->AddSlots();
        slot.SetId("static-slot");
        slot.SetIsDynamic(false);
        TString tenantName = cf.TenantName ? cf.TenantName.GetRef() : CanonizePath(DeduceNodeDomain(cf));
        slot.SetTenantName(tenantName);
        return tenantName;
    }

    void SetupLogConfigDefaults(NKikimrConfig::TLogConfig& logConfig) {
        ConfigFields.SetupLogConfigDefaults(logConfig, ConfigUpdateTracer.get());
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
        if (mem.HasLogBufferSize() && mem.GetLogBufferSize() > 0) {
            if (mem.HasLogGrainSize() && mem.GetLogGrainSize() > 0) {
                TMemoryLog::CreateMemoryLogBuffer(mem.GetLogBufferSize(), mem.GetLogGrainSize());
            } else {
                TMemoryLog::CreateMemoryLogBuffer(mem.GetLogBufferSize());
            }
            MemLogWriteNullTerm("Memory_log_has_been_started_YAHOO_");
        }
    }

    template <class TTag>
    void Option(const char* optname, TTag tag, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{*ConfigUpdateTracer, *ErrorCollector, *ProtoConfigFileProvider};
        MutableConfigPart(refs, optname, tag, BaseConfig, AppConfig, ctx);
    }

    template <class TTag, class TContinuation>
    void Option(const char* optname, TTag tag, TContinuation continuation, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{*ConfigUpdateTracer, *ErrorCollector, *ProtoConfigFileProvider};
        if (auto* res = MutableConfigPart(refs, optname, tag, BaseConfig, AppConfig, ctx)) {
            (this->*continuation)(*res);
        }
    }

    template <class TTag>
    void OptionMerge(const char* optname, TTag tag, NConfig::TCallContext ctx) {
        NConfig::TConfigRefs refs{*ConfigUpdateTracer, *ErrorCollector, *ProtoConfigFileProvider};
        MutableConfigPartMerge(refs, optname, tag, AppConfig, ctx);
    }

    void PreFillLabels(const NConfig::TConfigFields& cf) {
        Labels["node_id"] = ToString(cf.NodeId);
        Labels["node_host"] = Env->FQDNHostName();
        Labels["tenant"] = (cf.TenantName ? cf.TenantName.GetRef() : TString(""));
        Labels["node_type"] = cf.NodeType.GetRef();
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
        ConfigFields.SetupBootstrapConfigDefaults(bootstrapConfig, ConfigUpdateTracer.get());
    };

    void SetupInterconnectConfigDefaults(NKikimrConfig::TInterconnectConfig& icConfig) {
        ConfigFields.SetupInterconnectConfigDefaults(icConfig, ConfigUpdateTracer.get());
    };

    TString DeduceNodeDomain(const NConfig::TConfigFields& cf) const {
        if (cf.NodeDomain) {
            return cf.NodeDomain;
        }
        if (AppConfig.GetDomainsConfig().DomainSize() == 1) {
            return AppConfig.GetDomainsConfig().GetDomain(0).GetName();
        }
        if (AppConfig.GetTenantPoolConfig().SlotsSize() == 1) {
            auto &slot = AppConfig.GetTenantPoolConfig().GetSlots(0);
            if (slot.GetDomainName()) {
                return slot.GetDomainName();
            }
            auto &tenantName = slot.GetTenantName();
            if (IsStartWithSlash(tenantName)) {
                return ToString(ExtractDomain(tenantName));
            }
        }
        return "";
    }

    NYdb::NDiscovery::TNodeRegistrationResult TryToRegisterDynamicNodeViaDiscoveryService(
            const NConfig::TConfigFields& cf,
            const TString &addr,
            const TString &domainName,
            const TString &nodeHost,
            const TString &nodeAddress,
            const TString &nodeResolveHost,
            const TMaybe<TString>& path)
    {
        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
        NYdb::TDriverConfig config;
        if (endpoint.EnableSsl.Defined()) {
            if (cf.PathToGrpcCaFile) {
                config.UseSecureConnection(ReadFromFile(cf.PathToGrpcCaFile, "CA certificates").c_str());
            }
            if (cf.PathToGrpcCertFile && cf.PathToGrpcPrivateKeyFile) {
                auto certificate = ReadFromFile(cf.PathToGrpcCertFile, "Client certificates");
                auto privateKey = ReadFromFile(cf.PathToGrpcPrivateKeyFile, "Client certificates key");
                config.UseClientCertificate(certificate.c_str(), privateKey.c_str());
            }
        }
        config.SetAuthToken(BUILTIN_ACL_ROOT);
        config.SetEndpoint(endpoint.Address);
        auto connection = NYdb::TDriver(config);

        auto client = NYdb::NDiscovery::TDiscoveryClient(connection);
        NYdb::NDiscovery::TNodeRegistrationResult result = client.NodeRegistration(cf.GetNodeRegistrationSettings(domainName, nodeHost, nodeAddress, nodeResolveHost, path)).GetValueSync();
        connection.Stop(true);
        return result;
    }

    THolder<NClient::TRegistrationResult> TryToRegisterDynamicNodeViaLegacyService(
            const NConfig::TConfigFields& cf,
            const TString &addr,
            const TString &domainName,
            const TString &nodeHost,
            const TString &nodeAddress,
            const TString &nodeResolveHost,
            const TMaybe<TString>& path)
    {
        NClient::TKikimr kikimr(GetKikimr(cf, addr));
        auto registrant = kikimr.GetNodeRegistrant();

        auto loc = cf.CreateNodeLocation();

        return MakeHolder<NClient::TRegistrationResult>
            (registrant.SyncRegisterNode(ToString(domainName),
                                         nodeHost,
                                         cf.InterconnectPort,
                                         nodeAddress,
                                         nodeResolveHost,
                                         std::move(loc),
                                         cf.FixedNodeID,
                                         path));
    }

    NYdb::NDiscovery::TNodeRegistrationResult RegisterDynamicNodeViaDiscoveryService(
        const NConfig::TConfigFields& cf,
        const TVector<TString>& addrs,
        const TString& domainName)
    {
        NYdb::NDiscovery::TNodeRegistrationResult result;
        const size_t maxNumberReceivedCallUnimplemented = 5;
        size_t currentNumberReceivedCallUnimplemented = 0;
        while (!result.IsSuccess() && currentNumberReceivedCallUnimplemented < maxNumberReceivedCallUnimplemented) {
            for (const auto& addr : addrs) {
                result = TryToRegisterDynamicNodeViaDiscoveryService(
                    cf,
                    addr,
                    domainName,
                    cf.NodeHost,
                    cf.NodeAddress,
                    cf.NodeResolveHost,
                    cf.GetSchemePath());
                if (result.IsSuccess()) {
                    Cout << "Success. Registered via discovery service as " << result.GetNodeId() << Endl;
                    break;
                }
                Cerr << "Registration error: " << static_cast<NYdb::TStatus>(result) << Endl;
            }
            if (!result.IsSuccess()) {
                Sleep(TDuration::Seconds(1));
                if (result.GetStatus() == NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED) {
                    currentNumberReceivedCallUnimplemented++;
                }
            }
        }
        return result;
    }

    void ProcessRegistrationDynamicNodeResult(const NYdb::NDiscovery::TNodeRegistrationResult& result) {
        NodeId = result.GetNodeId();
        NActors::TScopeId scopeId;
        if (result.HasScopeTabletId() && result.HasScopePathId()) {
            scopeId.first = result.GetScopeTabletId();
            scopeId.second = result.GetScopePathId();
        }
        ScopeId = TKikimrScopeId(scopeId);

        auto &nsConfig = *AppConfig.MutableNameserviceConfig();
        nsConfig.ClearNode();

        auto &dnConfig = *AppConfig.MutableDynamicNodeConfig();
        for (auto &node : result.GetNodes()) {
            if (node.NodeId == result.GetNodeId()) {
                auto &nodeInfo = *dnConfig.MutableNodeInfo();
                nodeInfo.SetNodeId(node.NodeId);
                nodeInfo.SetHost(node.Host);
                nodeInfo.SetPort(node.Port);
                nodeInfo.SetResolveHost(node.ResolveHost);
                nodeInfo.SetAddress(node.Address);
                nodeInfo.SetExpire(node.Expire);
                NConfig::CopyNodeLocation(nodeInfo.MutableLocation(), node.Location);
            } else {
                auto &info = *nsConfig.AddNode();
                info.SetNodeId(node.NodeId);
                info.SetAddress(node.Address);
                info.SetPort(node.Port);
                info.SetHost(node.Host);
                info.SetInterconnectHost(node.ResolveHost);
                NConfig::CopyNodeLocation(info.MutableLocation(), node.Location);
            }
        }
    }

    THolder<NClient::TRegistrationResult> RegisterDynamicNodeViaLegacyService(
        const NConfig::TConfigFields& cf,
        const TVector<TString>& addrs,
        const TString& domainName)
    {
        THolder<NClient::TRegistrationResult> result;
        while (!result || !result->IsSuccess()) {
            for (const auto& addr : addrs) {
                result = TryToRegisterDynamicNodeViaLegacyService(
                    cf,
                    addr,
                    domainName,
                    cf.NodeHost,
                    cf.NodeAddress,
                    cf.NodeResolveHost,
                    cf.GetSchemePath());
                if (result->IsSuccess()) {
                    Cout << "Success. Registered via legacy service as " << result->GetNodeId() << Endl;
                    break;
                }
                Cerr << "Registration error: " << result->GetErrorMessage() << Endl;
            }
            if (!result || !result->IsSuccess())
                Sleep(TDuration::Seconds(1));
        }
        Y_ABORT_UNLESS(result);

        if (!result->IsSuccess()) {
            ythrow yexception() << "Cannot register dynamic node: " << result->GetErrorMessage();
        }

        return result;
    }

    void ProcessRegistrationDynamicNodeResult(const THolder<NClient::TRegistrationResult>& result) {
        NodeId = result->GetNodeId();
        ScopeId = TKikimrScopeId(result->GetScopeId());

        auto &nsConfig = *AppConfig.MutableNameserviceConfig();
        nsConfig.ClearNode();

        auto &dnConfig = *AppConfig.MutableDynamicNodeConfig();
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

    void RegisterDynamicNode(NConfig::TConfigFields& cf) {
        TVector<TString> addrs;

        cf.FillClusterEndpoints(AppConfig, addrs);

        if (!cf.InterconnectPort) {
            ythrow yexception() << "Either --node or --ic-port must be specified";
        }

        if (addrs.empty()) {
            ythrow yexception() << "List of Node Broker end-points is empty";
        }

        TString domainName = DeduceNodeDomain(cf);
        if (!cf.NodeHost) {
            cf.NodeHost = Env->FQDNHostName();
        }
        if (!cf.NodeResolveHost) {
            cf.NodeResolveHost = cf.NodeHost;
        }

        NYdb::NDiscovery::TNodeRegistrationResult result = RegisterDynamicNodeViaDiscoveryService(cf, addrs, domainName);
        if (result.IsSuccess()) {
            ProcessRegistrationDynamicNodeResult(result);
        } else {
            THolder<NClient::TRegistrationResult> result = RegisterDynamicNodeViaLegacyService(cf, addrs, domainName);
            ProcessRegistrationDynamicNodeResult(result);
        }
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

    bool TryToLoadConfigForDynamicNodeFromCMS(const NConfig::TConfigFields& cf, const TString &addr, TMaybe<NKikimr::NClient::TConfigurationResult>& res, TString &error) const {
        NClient::TKikimr kikimr(GetKikimr(cf, addr));
        auto configurator = kikimr.GetNodeConfigurator();

        Cout << "Trying to get configs from " << addr << Endl;

        auto result = configurator.SyncGetNodeConfig(NodeId,
                                                     Env->FQDNHostName(),
                                                     cf.TenantName.GetRef(),
                                                     cf.NodeType.GetRef(),
                                                     DeduceNodeDomain(cf),
                                                     AppConfig.GetAuthConfig().GetStaffApiUserToken(),
                                                     true,
                                                     1);

        if (!result.IsSuccess()) {
            error = result.GetErrorMessage();
            Cerr << "Configuration error: " << error << Endl;
            return false;
        }

        Cout << "Success." << Endl;

        res = result;

        return true;
    }

    void LoadConfigForDynamicNode(const NConfig::TConfigFields& cf, TMaybe<NKikimr::NClient::TConfigurationResult>& res) const {
        bool success = false;
        TString error;
        TVector<TString> addrs;

        cf.FillClusterEndpoints(AppConfig, addrs);

        SetRandomSeed(TInstant::Now().MicroSeconds());
        int minAttempts = 10;
        int attempts = 0;
        while (!success && attempts < minAttempts) {
            for (auto addr : addrs) {
                success = TryToLoadConfigForDynamicNodeFromCMS(cf, addr, res, error);
                ++attempts;
                if (success) {
                    break;
                }
            }
            // Randomized backoff
            if (!success) {
                Sleep(TDuration::MilliSeconds(500 + RandomNumber<ui64>(1000)));
            }
        }

        if (!success) {
            Cerr << "WARNING: couldn't load config from CMS: " << error << Endl;
        }
    }

    NClient::TKikimr GetKikimr(const NConfig::TConfigFields& cf, const TString& addr) const {
        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
        NYdbGrpc::TGRpcClientConfig grpcConfig(endpoint.Address, TDuration::Seconds(5));
        grpcConfig.LoadBalancingPolicy = "round_robin";
        if (endpoint.EnableSsl.Defined()) {
            grpcConfig.EnableSsl = endpoint.EnableSsl.GetRef();
            auto& sslCredentials = grpcConfig.SslCredentials;
            if (cf.PathToGrpcCaFile) {
                sslCredentials.pem_root_certs = ReadFromFile(cf.PathToGrpcCaFile, "CA certificates");
            }
            if (cf.PathToGrpcCertFile && cf.PathToGrpcPrivateKeyFile) {
                sslCredentials.pem_cert_chain = ReadFromFile(cf.PathToGrpcCertFile, "Client certificates");
                sslCredentials.pem_private_key = ReadFromFile(cf.PathToGrpcPrivateKeyFile, "Client certificates key");
            }
        }
        return NClient::TKikimr(grpcConfig);
    }
};

void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories) {
    parent.AddCommand(std::make_unique<TClientCommandServer>(factories));
}

} // namespace NKikimr::NDriverClient
