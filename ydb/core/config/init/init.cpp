#include "init.h"
#include "init_impl.h"

namespace NKikimr::NConfig {

class TDefaultEnv
    : public IEnv
{
public:
    TString HostName() const override {
        return ::HostName();
    }

    TString FQDNHostName() const override {
        return ::FQDNHostName();
    }

    TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty) const override {
        return ::ReadFromFile(filePath, fileName, allowEmpty);
    }

    void Sleep(const TDuration& dur) const override {
        ::Sleep(dur);
    }
};

class TDefaultErrorCollector
    : public IErrorCollector
{
public:
    void Fatal(TString error) override {
        Cerr << error << Endl;
    }
};

struct TFileConfigOptions {
    TString Description;
    TMaybe<TString> ParsedOption;
};

class TDefaultProtoConfigFileProvider
    : public IProtoConfigFileProvider
{
private:
    TMap<TString, TSimpleSharedPtr<TFileConfigOptions>> Opts;

    static bool IsFileExists(const fs::path& p) {
        std::error_code ec;
        return fs::exists(p, ec) && !ec;
    }

    static bool IsFileReadable(const fs::path& p) {
        std::error_code ec; // For noexcept overload usage.
        auto perms = fs::status(p, ec).permissions();
        if ((perms & fs::perms::owner_read) != fs::perms::none &&
            (perms & fs::perms::group_read) != fs::perms::none &&
            (perms & fs::perms::others_read) != fs::perms::none
            )
        {
            return true;
        }
        return false;
    }
public:
    TDefaultProtoConfigFileProvider() {
        AddProtoConfigOptions(*this);
    }

    void AddConfigFile(TString optName, TString description) override {
        Opts.emplace(optName, MakeSimpleShared<TFileConfigOptions>(TFileConfigOptions{.Description = description}));
    }

    void RegisterCliOptions(NLastGetopt::TOpts& opts) const override {
        for (const auto& [name, opt] : Opts) {
            opts.AddLongOption(name, opt->Description).OptionalArgument("PATH").StoreResult(&opt->ParsedOption);
        }
    }

    TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const override {
        fs::path filePath(path.c_str());
        if (!IsFileExists(filePath)) {
            errorCollector.Fatal(Sprintf("File %s doesn't exists", path.c_str()));
            return {};
        }
        if (!IsFileReadable(filePath)) {
            errorCollector.Fatal(Sprintf("File %s isn't readable", path.c_str()));
            return {};
        }
        TAutoPtr<TMappedFileInput> fileInput(new TMappedFileInput(path));
        return fileInput->ReadAll();
    }

    bool Has(TString optName) override {
        if (auto* opt = Opts.FindPtr(optName)) {
            return !!((*opt)->ParsedOption);
        }
        return false;
    }

    TString Get(TString optName) override {
        if (auto* opt = Opts.FindPtr(optName); opt && (*opt)->ParsedOption) {
            return (*opt)->ParsedOption.GetRef();
        }
        return ""; // FIXME: throw
    }
};

class TDefaultConfigUpdateTracer
    : public IConfigUpdateTracer
{
private:
    THashMap<ui32, TConfigItemInfo> ConfigInitInfo;

public:
    void Add(ui32 kind, TConfigItemInfo::TUpdate update) override {
        ConfigInitInfo[kind].Updates.emplace_back(update);
    }

    THashMap<ui32, TConfigItemInfo> Dump() const override {
        return ConfigInitInfo;
    }
};

class TDefaultMemLogInitializer
    : public IMemLogInitializer
{
public:
    void Init(const NKikimrConfig::TMemoryLogConfig& mem) const override {
        if (mem.HasLogBufferSize() && mem.GetLogBufferSize() > 0) {
            if (mem.HasLogGrainSize() && mem.GetLogGrainSize() > 0) {
                TMemoryLog::CreateMemoryLogBuffer(mem.GetLogBufferSize(), mem.GetLogGrainSize());
            } else {
                TMemoryLog::CreateMemoryLogBuffer(mem.GetLogBufferSize());
            }
            MemLogWriteNullTerm("Memory_log_has_been_started_YAHOO_");
        }
    }
};

class TDefaultNodeBrokerClient
    : public INodeBrokerClient
{
    class TResult
        : public INodeRegistrationResult
    {
        static void ProcessRegistrationDynamicNodeResult(
            const NYdb::NDiscovery::TNodeRegistrationResult& result,
            NKikimrConfig::TAppConfig& appConfig,
            ui32& nodeId,
            TKikimrScopeId& outScopeId)
        {
            nodeId = result.GetNodeId();
            NActors::TScopeId scopeId;
            if (result.HasScopeTabletId() && result.HasScopePathId()) {
                scopeId.first = result.GetScopeTabletId();
                scopeId.second = result.GetScopePathId();
            }
            outScopeId = TKikimrScopeId(scopeId);

            auto &nsConfig = *appConfig.MutableNameserviceConfig();
            nsConfig.ClearNode();

            auto &dnConfig = *appConfig.MutableDynamicNodeConfig();
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

        static void ProcessRegistrationDynamicNodeResult(
            const THolder<NClient::TRegistrationResult>& result,
            NKikimrConfig::TAppConfig& appConfig,
            ui32& nodeId,
            TKikimrScopeId& outScopeId)
        {
            nodeId = result->GetNodeId();
            outScopeId = TKikimrScopeId(result->GetScopeId());

            auto &nsConfig = *appConfig.MutableNameserviceConfig();
            nsConfig.ClearNode();

            auto &dnConfig = *appConfig.MutableDynamicNodeConfig();
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

        std::variant<
            NYdb::NDiscovery::TNodeRegistrationResult,
            THolder<NClient::TRegistrationResult>> Result;
    public:
        TResult(std::variant<
            NYdb::NDiscovery::TNodeRegistrationResult,
            THolder<NClient::TRegistrationResult>> result)
            : Result(std::move(result))
        {}

        void Apply(NKikimrConfig::TAppConfig& appConfig, ui32& nodeId, TKikimrScopeId& scopeId) const override {
            std::visit([&appConfig, &nodeId, &scopeId](const auto& res) mutable { ProcessRegistrationDynamicNodeResult(res, appConfig, nodeId, scopeId); }, Result);
        }
    };

    static NYdb::NDiscovery::TNodeRegistrationResult TryToRegisterDynamicNodeViaDiscoveryService(
            const TGrpcSslSettings& gs,
            const TString addr,
            const NYdb::NDiscovery::TNodeRegistrationSettings& nrs,
            const IEnv& env)
    {
        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
        NYdb::TDriverConfig config;
        if (endpoint.EnableSsl.Defined()) {
            if (gs.PathToGrpcCaFile) {
                config.UseSecureConnection(env.ReadFromFile(gs.PathToGrpcCaFile, "CA certificates").c_str());
            }
            if (gs.PathToGrpcCertFile && gs.PathToGrpcPrivateKeyFile) {
                auto certificate = env.ReadFromFile(gs.PathToGrpcCertFile, "Client certificates");
                auto privateKey = env.ReadFromFile(gs.PathToGrpcPrivateKeyFile, "Client certificates key");
                config.UseClientCertificate(certificate.c_str(), privateKey.c_str());
            }
        }
        config.SetAuthToken(BUILTIN_ACL_ROOT);
        config.SetEndpoint(endpoint.Address);
        auto connection = NYdb::TDriver(config);

        auto client = NYdb::NDiscovery::TDiscoveryClient(connection);
        NYdb::NDiscovery::TNodeRegistrationResult result = client.NodeRegistration(nrs).GetValueSync();
        connection.Stop(true);
        return result;
    }

    static THolder<NClient::TRegistrationResult> TryToRegisterDynamicNodeViaLegacyService(
            const TGrpcSslSettings& rgs,
            const TString& addr,
            const TNodeRegistrationSettings& rs,
            const IEnv& env)
    {
        NClient::TKikimr kikimr(GetKikimr(rgs, addr, env));
        auto registrant = kikimr.GetNodeRegistrant();

        return MakeHolder<NClient::TRegistrationResult>(
            registrant.SyncRegisterNode(
                ToString(rs.DomainName),
                rs.NodeHost,
                rs.InterconnectPort,
                rs.NodeAddress,
                rs.NodeResolveHost,
                rs.Location,
                rs.FixedNodeID,
                rs.Path));
    }

    static THolder<NClient::TRegistrationResult> RegisterDynamicNodeViaLegacyService(
        const TGrpcSslSettings& gs,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& rs,
        const IEnv& env)
    {
        THolder<NClient::TRegistrationResult> result;
        while (!result || !result->IsSuccess()) {
            for (const auto& addr : addrs) {
                result = TryToRegisterDynamicNodeViaLegacyService(
                    gs,
                    addr,
                    rs,
                    env);
                if (result->IsSuccess()) {
                    Cout << "Success. Registered via legacy service as " << result->GetNodeId() << Endl;
                    break;
                }
                Cerr << "Registration error: " << result->GetErrorMessage() << Endl;
            }
            if (!result || !result->IsSuccess()) {
                env.Sleep(TDuration::Seconds(1));
            }
        }
        if (!result) {
            ythrow yexception() << "Invalid result";
        }

        if (!result->IsSuccess()) {
            ythrow yexception() << "Cannot register dynamic node: " << result->GetErrorMessage();
        }

        return result;
    }

   static  NYdb::NDiscovery::TNodeRegistrationResult RegisterDynamicNodeViaDiscoveryService(
        const TGrpcSslSettings& gs,
        const TVector<TString>& addrs,
        const NYdb::NDiscovery::TNodeRegistrationSettings& nrs,
        const IEnv& env)
    {
        NYdb::NDiscovery::TNodeRegistrationResult result;
        const size_t maxNumberReceivedCallUnimplemented = 5;
        size_t currentNumberReceivedCallUnimplemented = 0;
        while (!result.IsSuccess() && currentNumberReceivedCallUnimplemented < maxNumberReceivedCallUnimplemented) {
            for (const auto& addr : addrs) {
                result = TryToRegisterDynamicNodeViaDiscoveryService(
                    gs,
                    addr,
                    nrs,
                    env);
                if (result.IsSuccess()) {
                    Cout << "Success. Registered via discovery service as " << result.GetNodeId() << Endl;
                    break;
                }
                Cerr << "Registration error: " << static_cast<NYdb::TStatus>(result) << Endl;
            }
            if (!result.IsSuccess()) {
                env.Sleep(TDuration::Seconds(1));
                if (result.GetStatus() == NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED) {
                    currentNumberReceivedCallUnimplemented++;
                }
            }
        }
        return result;
    }

    static NYdb::NDiscovery::TNodeRegistrationSettings GetNodeRegistrationSettings(const TNodeRegistrationSettings& rs)
    {
        NYdb::NDiscovery::TNodeRegistrationSettings settings;
        settings.Host(rs.NodeHost);
        settings.Port(rs.InterconnectPort);
        settings.ResolveHost(rs.NodeResolveHost);
        settings.Address(rs.NodeAddress);
        settings.DomainPath(rs.DomainName);
        settings.FixedNodeId(rs.FixedNodeID);
        if (rs.Path) {
            settings.Path(*rs.Path);
        }

        auto loc = rs.Location;
        NActorsInterconnect::TNodeLocation tmpLocation;
        loc.Serialize(&tmpLocation, false);

        NYdb::NDiscovery::TNodeLocation settingLocation;
        CopyNodeLocation(&settingLocation, tmpLocation);
        settings.Location(settingLocation);
        return settings;
    }

public:
    std::unique_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& regSettings,
        const IEnv& env) const override
    {
        auto newRegSettings = GetNodeRegistrationSettings(regSettings);

        std::variant<
            NYdb::NDiscovery::TNodeRegistrationResult,
            THolder<NClient::TRegistrationResult>> result = RegisterDynamicNodeViaDiscoveryService(
                grpcSettings,
                addrs,
                newRegSettings,
                env);

        if (!std::get<NYdb::NDiscovery::TNodeRegistrationResult>(result).IsSuccess()) {
            result = RegisterDynamicNodeViaLegacyService(
                grpcSettings,
                addrs,
                regSettings,
                env);
        }

        return std::make_unique<TResult>(std::move(result));
    }
};

std::unique_ptr<IEnv> MakeDefaultEnv() {
    return std::make_unique<TDefaultEnv>();
}

std::unique_ptr<IErrorCollector> MakeDefaultErrorCollector() {
    return std::make_unique<TDefaultErrorCollector>();
}

std::unique_ptr<IProtoConfigFileProvider> MakeDefaultProtoConfigFileProvider() {
    return std::make_unique<TDefaultProtoConfigFileProvider>();
}

std::unique_ptr<IConfigUpdateTracer> MakeDefaultConfigUpdateTracer() {
    return std::make_unique<TDefaultConfigUpdateTracer>();
}

std::unique_ptr<IMemLogInitializer> MakeDefaultMemLogInitializer() {
    return std::make_unique<TDefaultMemLogInitializer>();
}

std::unique_ptr<INodeBrokerClient> MakeDefaultNodeBrokerClient() {
    return std::make_unique<TDefaultNodeBrokerClient>();
}

void CopyNodeLocation(NActorsInterconnect::TNodeLocation* dst, const NYdb::NDiscovery::TNodeLocation& src) {
    if (src.DataCenterNum) {
        dst->SetDataCenterNum(src.DataCenterNum.value());
    }
    if (src.RoomNum) {
        dst->SetRoomNum(src.RoomNum.value());
    }
    if (src.RackNum) {
        dst->SetRackNum(src.RackNum.value());
    }
    if (src.BodyNum) {
        dst->SetBodyNum(src.BodyNum.value());
    }
    if (src.Body) {
        dst->SetBody(src.Body.value());
    }
    if (src.DataCenter) {
        dst->SetDataCenter(src.DataCenter.value());
    }
    if (src.Module) {
        dst->SetModule(src.Module.value());
    }
    if (src.Rack) {
        dst->SetRack(src.Rack.value());
    }
    if (src.Unit) {
        dst->SetUnit(src.Unit.value());
    }
}

void CopyNodeLocation(NYdb::NDiscovery::TNodeLocation* dst, const NActorsInterconnect::TNodeLocation& src) {
    if (src.HasDataCenterNum()) {
        dst->DataCenterNum = src.GetDataCenterNum();
    }
    if (src.HasRoomNum()) {
        dst->RoomNum = src.GetRoomNum();
    }
    if (src.HasRackNum()) {
        dst->RackNum = src.GetRackNum();
    }
    if (src.HasBodyNum()) {
        dst->BodyNum = src.GetBodyNum();
    }
    if (src.HasBody()) {
        dst->Body = src.GetBody();
    }
    if (src.HasDataCenter()) {
        dst->DataCenter = src.GetDataCenter();
    }
    if (src.HasModule()) {
        dst->Module = src.GetModule();
    }
    if (src.HasRack()) {
        dst->Rack = src.GetRack();
    }
    if (src.HasUnit()) {
        dst->Unit = src.GetUnit();
    }
}

void AddProtoConfigOptions(IProtoConfigFileProvider& out) {
    const TMap<TString, TString> opts = {
        {"alloc-file", "Allocator config file"},
        {"audit-file", "File with audit config"},
        {"auth-file", "authorization configuration"},
        {"auth-token-file", "authorization token configuration"},
        {"bootstrap-file", "Bootstrap config file"},
        {"bs-file", "blobstorage config file"},
        {"channels-file", "tablet channel profile config file"},
        {"cms-file", "CMS config file"},
        {"domains-file", "domain config file"},
        {"drivemodel-file", "drive model config file"},
        {"dyn-nodes-file", "Dynamic nodes config file"},
        {"feature-flags-file", "File with feature flags to turn new features on/off"},
        {"fq-file", "Federated Query config file"},
        {"grpc-file", "gRPC config file"},
        {"http-proxy-file", "Http proxy config file"},
        {"ic-file", "interconnect config file"},
        {"incrhuge-file", "incremental huge blob keeper config file"},
        {"key-file", "tenant encryption key configuration"},
        {"kqp-file", "Kikimr Query Processor config file"},
        {"log-file", "log config file"},
        {"memorylog-file", "set buffer size for memory log"},
        {"metering-file", "File with metering config"},
        {"naming-file", "static nameservice config file"},
        {"netclassifier-file", "NetClassifier config file"},
        {"pdisk-key-file", "pdisk encryption key configuration"},
        {"pq-file", "PersQueue config file"},
        {"pqcd-file", "PersQueue cluster discovery config file"},
        {"public-http-file", "Public HTTP config file"},
        {"rb-file", "File with resource broker customizations"},
        {"sqs-file", "SQS config file"},
        {"sys-file", "actor system config file (use dummy config by default)"},
        {"vdisk-file", "vdisk kind config file"},
    };

    for (const auto& [opt, desc] : opts) {
        out.AddConfigFile(opt, desc);
    }
}

void LoadBootstrapConfig(IProtoConfigFileProvider& protoConfigFileProvider, IErrorCollector& errorCollector, TVector<TString> configFiles, NKikimrConfig::TAppConfig& out) {
    for (const TString& path : configFiles) {
        NKikimrConfig::TAppConfig parsedConfig;
        const TString protoString = protoConfigFileProvider.GetProtoFromFile(path, errorCollector);
        /*
         * FIXME: if (ErrorCollector.HasFatal()) { return; }
         */
        const bool result = ParsePBFromString(protoString, &parsedConfig);
        if (!result) {
            errorCollector.Fatal(Sprintf("Can't parse protobuf: %s", path.c_str()));
            return;
        }
        out.MergeFrom(parsedConfig);
    }
}

void LoadYamlConfig(TConfigRefs refs, const TString& yamlConfigFile, NKikimrConfig::TAppConfig& appConfig, TCallContext callCtx) {
    if (!yamlConfigFile) {
        return;
    }

    IConfigUpdateTracer& ConfigUpdateTracer = refs.Tracer;
    IErrorCollector& errorCollector = refs.ErrorCollector;
    IProtoConfigFileProvider& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    const TString yamlConfigString = protoConfigFileProvider.GetProtoFromFile(yamlConfigFile, errorCollector);
    /*
     * FIXME: if (ErrorCollector.HasFatal()) { return; }
     */
    NKikimrConfig::TAppConfig parsedConfig = NKikimr::NYaml::Parse(yamlConfigString); // FIXME
    /*
     * FIXME: if (ErrorCollector.HasFatal()) { return; }
     */
    const google::protobuf::Descriptor* descriptor = appConfig.GetDescriptor();
    const google::protobuf::Reflection* reflection = appConfig.GetReflection();
    for(int fieldIdx = 0; fieldIdx < descriptor->field_count(); ++fieldIdx) {
        const google::protobuf::FieldDescriptor* fieldDescriptor = descriptor->field(fieldIdx);
        if (!fieldDescriptor) {
            continue;
        }

        if (fieldDescriptor->is_repeated()) {
            continue;
        }

        if (reflection->HasField(appConfig, fieldDescriptor)) {
            // field is already set in app config
            continue;
        }

        if (reflection->HasField(parsedConfig, fieldDescriptor)) {
            reflection->SwapFields(&appConfig, &parsedConfig, {fieldDescriptor});
            TRACE_CONFIG_CHANGE(callCtx, fieldIdx, ReplaceConfigWithConsoleProto);
        }
    }
}

} // namespace NKikimr::NConfig
