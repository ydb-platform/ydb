#include "init_impl.h"
#include "mock.h"
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <ydb/core/util/backoff.h>

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
    // TODO(Enjection): CFG-UX-0 replace regular throw with just collecting
    void Fatal(TString error) override {
        ythrow yexception() << error;
    }
};

class TDefaultProtoConfigFileProvider
    : public IProtoConfigFileProvider
{
private:
    TMap<TString, TSimpleSharedPtr<TFileConfigOptions>> Opts;

    static bool IsFileReadable(const fs::path& p) {
        std::error_code ec; // For noexcept overload usage.
        auto perms = fs::status(p, ec).permissions();
        if ((perms & fs::perms::owner_read)  != fs::perms::none ||
            (perms & fs::perms::group_read)  != fs::perms::none ||
            (perms & fs::perms::others_read) != fs::perms::none   )
        {
            return true;
        }

        return false;
    }
public:
    void AddConfigFile(TString optName, TString description) override {
        Opts.emplace(optName, MakeSimpleShared<TFileConfigOptions>(TFileConfigOptions{.Description = description}));
    }

    void RegisterCliOptions(NLastGetopt::TOpts& opts) const override {
        for (const auto& [name, opt] : Opts) {
            opts.AddLongOption(name, opt->Description)
                .OptionalArgument("PATH")
                .StoreResult(&opt->ParsedOption);
        }
    }

    TString GetProtoFromFile(const TString& path, IErrorCollector& errorCollector) const override {
        fs::path filePath(path.c_str());
        if (!fs::is_regular_file(filePath)) {
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
        // TODO(Enjection): CFG-UX-0 replace with IErrorCollector call
        ythrow yexception() << "option " << optName.Quote() << " undefined";
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
            TKikimrScopeId& outScopeId,
            TString& outNodeName)
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
                    nodeInfo.SetHost(TString{node.Host});
                    nodeInfo.SetPort(node.Port);
                    nodeInfo.SetResolveHost(TString{node.ResolveHost});
                    nodeInfo.SetAddress(TString{node.Address});
                    nodeInfo.SetExpire(node.Expire);
                    NConfig::CopyNodeLocation(nodeInfo.MutableLocation(), node.Location);
                    if (result.HasNodeName()) {
                        nodeInfo.SetName(TString{result.GetNodeName()});
                        outNodeName = result.GetNodeName();
                    }
                    if (node.BridgePileId) {
                        nodeInfo.SetBridgePileId(*node.BridgePileId);
                    }
                } else {
                    auto &info = *nsConfig.AddNode();
                    info.SetNodeId(node.NodeId);
                    info.SetAddress(TString{node.Address});
                    info.SetPort(node.Port);
                    info.SetHost(TString{node.Host});
                    info.SetInterconnectHost(TString{node.ResolveHost});
                    NConfig::CopyNodeLocation(info.MutableLocation(), node.Location);
                    if (node.BridgePileId && appConfig.HasBridgeConfig()) {
                        info.SetBridgePileName(appConfig.GetBridgeConfig().GetPiles(*node.BridgePileId).GetName());
                    }
                }
            }
        }

            NYdb::NDiscovery::TNodeRegistrationResult Result;
    public:
        TResult(NYdb::NDiscovery::TNodeRegistrationResult result)
            : Result(std::move(result))
        {}

        void Apply(
            NKikimrConfig::TAppConfig& appConfig,
            ui32& nodeId,
            TKikimrScopeId& scopeId,
            TString& nodeName) const override
        {
            ProcessRegistrationDynamicNodeResult(Result, appConfig, nodeId, scopeId, nodeName);
        }
    };

    static NYdb::NDiscovery::TNodeRegistrationResult TryToRegisterDynamicNode(
            const TGrpcSslSettings& grpcSettings,
            const TString addr,
            const NYdb::NDiscovery::TNodeRegistrationSettings& settings,
            const TString& nodeRegistrationToken,
            const IEnv& env)
    {
        NYdb::TDriverConfig config = CreateDriverConfig(grpcSettings, addr, env, nodeRegistrationToken);
        auto connection = NYdb::TDriver(config);

        auto client = NYdb::NDiscovery::TDiscoveryClient(connection);
        NYdb::NDiscovery::TNodeRegistrationResult result = client.NodeRegistration(settings).GetValueSync();
        connection.Stop(true);
        return result;
    }

   static NYdb::NDiscovery::TNodeRegistrationResult RegisterDynamicNodeImpl(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const NYdb::NDiscovery::TNodeRegistrationSettings& settings,
        const TString& nodeRegistrationToken,
        const IEnv& env,
        IInitLogger& logger)
    {
        NYdb::NDiscovery::TNodeRegistrationResult result;
        while (!result.IsSuccess()) {
            for (const auto& addr : addrs) {
                logger.Out() << "Trying to register dynamic node to " << addr << Endl;
                result = TryToRegisterDynamicNode(grpcSettings,
                                                  addr,
                                                  settings,
                                                  nodeRegistrationToken,
                                                  env);
                if (result.IsSuccess()) {
                    logger.Out() << "Success. Registered as " << result.GetNodeId() << Endl;
                    if (result.HasNodeName()) {
                        logger.Out() << "Node name: " << result.GetNodeName() << Endl;
                    }
                    break;
                }
                logger.Err() << "Registration error: " << static_cast<NYdb::TStatus>(result) << Endl;
            }
            if (!result.IsSuccess()) {
                env.Sleep(TDuration::Seconds(1));
            }
        }
        return result;
    }

    static NYdb::NDiscovery::TNodeRegistrationSettings GetNodeRegistrationSettings(const TNodeRegistrationSettings& settings)
    {
        NYdb::NDiscovery::TNodeRegistrationSettings result;
        result.Host(settings.NodeHost);
        result.Port(settings.InterconnectPort);
        result.ResolveHost(settings.NodeResolveHost);
        result.Address(settings.NodeAddress);
        result.DomainPath(settings.DomainName);
        result.FixedNodeId(settings.FixedNodeID);
        if (settings.Path) {
            result.Path(*settings.Path);
        }
        if (settings.BridgePileName) {
            result.BridgePileName(*settings.BridgePileName);
        }

        auto loc = settings.Location;
        NActorsInterconnect::TNodeLocation tmpLocation;
        loc.Serialize(&tmpLocation, false);

        NYdb::NDiscovery::TNodeLocation settingLocation;
        CopyNodeLocation(&settingLocation, tmpLocation);
        result.Location(settingLocation);
        return result;
    }

public:
    std::shared_ptr<INodeRegistrationResult> RegisterDynamicNode(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TNodeRegistrationSettings& regSettings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        auto newRegSettings = GetNodeRegistrationSettings(regSettings);

       NYdb::NDiscovery::TNodeRegistrationResult result = RegisterDynamicNodeImpl(grpcSettings,
                                                                                  addrs,
                                                                                  newRegSettings,
                                                                                  regSettings.NodeRegistrationToken,
                                                                                  env,
                                                                                  logger);

        return std::make_shared<TResult>(std::move(result));
    }
};

class TDynConfigResultWrapper
    : public IConfigurationResult
{
    NKikimr::NClient::TConfigurationResult Result;
public:
    TDynConfigResultWrapper(NKikimr::NClient::TConfigurationResult&& result)
        : Result(std::move(result))
    {}

    const NKikimrConfig::TAppConfig& GetConfig() const override {
        return Result.GetConfig();
    }

    bool HasMainYamlConfig() const override {
        return Result.HasMainYamlConfig();
    }

    const TString& GetMainYamlConfig() const override {
        return Result.GetMainYamlConfig();
    }

    TMap<ui64, TString> GetVolatileYamlConfigs() const override {
        return Result.GetVolatileYamlConfigs();
    }

    bool HasDatabaseYamlConfig() const override {
        return Result.HasDatabaseYamlConfig();
    }

    const TString& GetDatabaseYamlConfig() const override {
        return Result.GetDatabaseYamlConfig();
    }
};

namespace {

struct RetryResult {
    bool Success;
    int TotalAttempts;
    int Rounds;
};

template <typename TAttemptFn>
RetryResult RetryWithJitter(
    const TVector<TString>& addrs,
    const IEnv& env,
    TAttemptFn attempt)
{
    const int maxRounds = 10;
    const TDuration baseRoundDelay = TDuration::MilliSeconds(500);
    const TDuration maxIntraAddrDelay = TDuration::Minutes(3);
    const TDuration maxDelay = TDuration::Minutes(5);
    const TDuration baseAddressDelay = TDuration::MilliSeconds(250);

    auto sleepWithJitteredExponentialDelay = [&env](TDuration baseDelay, TDuration maxDelay, int exponent) {
        ui64 multiplier = 1ULL << exponent;
        TDuration delay = baseDelay * multiplier;
        delay = Min(delay, maxDelay);
        
        ui64 maxMs = delay.MilliSeconds();
        ui64 jitteredMs = RandomNumber<ui64>(maxMs + 1);
        TDuration jitteredDelay = TDuration::MilliSeconds(jitteredMs);
        
        env.Sleep(jitteredDelay);
    };

    int round = 0;
    int totalAttempts = 0;
    bool success = false;

    while (!success && round < maxRounds) {
        int addressIndex = 0;
        for (const auto& addr : addrs) {
            success = attempt(addr);
            ++totalAttempts;
            
            if (success) {
                break;
            }
            
            // Exponential delay between individual addresses - delay grows with each address in the round
            if (addrs.size() > 1) {
                sleepWithJitteredExponentialDelay(baseAddressDelay, maxIntraAddrDelay, Max(addressIndex, round));
            }
            
            ++addressIndex;
        }
        
        if (!success) {
            ++round;
            
            if (round < maxRounds) {
                sleepWithJitteredExponentialDelay(baseRoundDelay, maxDelay, round - 1);
            }
        }
    }
    return {success, totalAttempts, round};
}

} // namespace

class TDefaultDynConfigClient
    : public IDynConfigClient
{
    static bool TryToLoadConfigForDynamicNodeFromCMS(
        const TGrpcSslSettings& grpcSettings,
        const TString &addr,
        const TDynConfigSettings& settings,
        const IEnv& env,
        IInitLogger& logger,
        std::shared_ptr<IConfigurationResult>& res,
        TString &error)
    {
        NClient::TKikimr kikimr(GetKikimr(
                    grpcSettings,
                    addr,
                    env));
        auto configurator = kikimr.GetNodeConfigurator();

        logger.Out() << "Trying to get configs from " << addr << Endl;

        auto result = configurator.SyncGetNodeConfig(settings.NodeId,
                                                     settings.FQDNHostName,
                                                     settings.TenantName,
                                                     settings.NodeType,
                                                     settings.DomainName,
                                                     settings.StaffApiUserToken,
                                                     true,
                                                     1);

        if (!result.IsSuccess()) {
            res = nullptr;
            error = result.GetErrorMessage();
            logger.Err() << "Configuration error: " << error << Endl;
            return false;
        }

        logger.Out() << "Success." << Endl;

        res = std::make_shared<TDynConfigResultWrapper>(std::move(result));

        return true;
    }
public:
    std::shared_ptr<IConfigurationResult> GetConfig(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const TDynConfigSettings& settings,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        std::shared_ptr<IConfigurationResult> res;
        TString error;
        SetRandomSeed(TInstant::Now().MicroSeconds());

        auto attempt = [&](const TString& addr) {
            logger.Out() << "Trying to get dynamic config from " << addr << Endl;
            bool success = TryToLoadConfigForDynamicNodeFromCMS(grpcSettings, addr, settings, env, logger, res, error);
            if (success) {
                logger.Out() << "Success. Got dynamic config from " << addr << Endl;
            }
            return success;
        };

        const auto result = RetryWithJitter(addrs, env, attempt);

        if (!result.Success) {
            logger.Err() << "WARNING: couldn't load config from Console after " 
                        << result.TotalAttempts << " attempts across " << result.Rounds 
                        << " rounds: " << error << Endl;
        }

        return res;
    }
};

class TConfigResultWrapper
    : public IStorageConfigResult
{
public:
    TConfigResultWrapper(const NYdb::NConfig::TFetchConfigResult& result) {
        TString clusterConfig;
        TString storageConfig;
        for (const auto& entry : result.GetConfigs()) {
            std::visit([&](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, NYdb::NConfig::TMainConfigIdentity>) {
                    MainYamlConfig = entry.Config;
                } else if constexpr (std::is_same_v<T, NYdb::NConfig::TStorageConfigIdentity>) {
                    StorageYamlConfig = entry.Config;
                }
            }, entry.Identity);
        }
    }

    const TString& GetMainYamlConfig() const override {
        return MainYamlConfig;
    }

    const TString& GetStorageYamlConfig() const override {
        return StorageYamlConfig;
    }

private:
    TString MainYamlConfig;
    TString StorageYamlConfig;
};

class TDefaultConfigClient
    : public IConfigClient
{
private:
    static NYdb::NConfig::TFetchConfigResult TryToFetchConfig(
        const TGrpcSslSettings& grpcSettings,
        const TString& addrs,
        const IEnv& env)
    {

        NYdb::TDriverConfig config = CreateDriverConfig(grpcSettings, addrs, env);

        auto connection = NYdb::TDriver(config);

        auto client = NYdb::NConfig::TConfigClient(connection);
        NYdb::NConfig::TFetchConfigResult result = client.FetchAllConfigs().GetValueSync();
        connection.Stop(true);
        return result;
    }

    static NYdb::NConfig::TFetchConfigResult FetchConfigImpl(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const IEnv& env,
        IInitLogger& logger)
    {
        std::optional<NYdb::NConfig::TFetchConfigResult> result;
        SetRandomSeed(TInstant::Now().MicroSeconds());

        auto attempt = [&](const TString& addr) {
            logger.Out() << "Trying to fetch config from " << addr << Endl;
            result = TryToFetchConfig(grpcSettings, addr, env);
            if (result->IsSuccess()) {
                logger.Out() << "Success. Fetched config from " << addr << Endl;
                return true;
            }
            logger.Err() << "Fetch config error: " << static_cast<NYdb::TStatus>(*result) << Endl;
            return false;
        };

        const auto retryResult = RetryWithJitter(addrs, env, attempt);

        if (!retryResult.Success) {
             logger.Err() << "WARNING: couldn't fetch config from Console after " 
                        << retryResult.TotalAttempts << " attempts across " << retryResult.Rounds 
                        << " rounds. Last error: " << static_cast<NYdb::TStatus>(*result) << Endl;
        }
        return *result;
    }

public:
    std::shared_ptr<IStorageConfigResult> FetchConfig(
        const TGrpcSslSettings& grpcSettings,
        const TVector<TString>& addrs,
        const IEnv& env,
        IInitLogger& logger) const override
    {
        auto result = FetchConfigImpl(grpcSettings, addrs, env, logger);
        if (!result.IsSuccess()) {
            return nullptr;
        }
        return std::make_shared<TConfigResultWrapper>(std::move(result));
    }
};

class TDefaultInitLogger
    : public IInitLogger
{
public:
    IOutputStream& Out() const noexcept override {
        return Cout;
    }

    IOutputStream& Err() const noexcept override {
        return Cerr;
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

std::unique_ptr<IDynConfigClient> MakeDefaultDynConfigClient() {
    return std::make_unique<TDefaultDynConfigClient>();
}

std::unique_ptr<IConfigClient> MakeDefaultConfigClient() {
    return std::make_unique<TDefaultConfigClient>();
}

std::unique_ptr<IInitLogger> MakeDefaultInitLogger() {
    return std::make_unique<TDefaultInitLogger>();
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
        dst->SetDataCenter(TString{src.DataCenter.value()});
    }
    if (src.Module) {
        dst->SetModule(TString{src.Module.value()});
    }
    if (src.Rack) {
        dst->SetRack(TString{src.Rack.value()});
    }
    if (src.Unit) {
        dst->SetUnit(TString{src.Unit.value()});
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

void LoadMainYamlConfig(
    TConfigRefs refs,
    const TString& mainYamlConfigFile,
    const TString& storageYamlConfigFile,
    bool loadedFromStore,
    NKikimrConfig::TAppConfig& appConfig,
    NYamlConfig::IConfigSwissKnife* csk,
    const NCompat::TSourceLocation location)
{
    if (!mainYamlConfigFile) {
        return;
    }

    IConfigUpdateTracer& configUpdateTracer = refs.Tracer;
    IErrorCollector& errorCollector = refs.ErrorCollector;
    IProtoConfigFileProvider& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    std::optional<TString> storageYamlConfigString;
    if (storageYamlConfigFile) {
        storageYamlConfigString.emplace(protoConfigFileProvider.GetProtoFromFile(storageYamlConfigFile, errorCollector));

        if (csk) {
            csk->VerifyStorageConfig(*storageYamlConfigString);
        }
    }

    const TString mainYamlConfigString = protoConfigFileProvider.GetProtoFromFile(mainYamlConfigFile, errorCollector);

    if (csk) {
        csk->VerifyMainConfig(mainYamlConfigString);
    }

    appConfig.SetStartupConfigYaml(mainYamlConfigString);
    if (storageYamlConfigString) {
        appConfig.SetStartupStorageYaml(*storageYamlConfigString);
    }

    if (loadedFromStore) {
        auto *yamlConfig = appConfig.MutableStoredConfigYaml();
        yamlConfig->SetMainConfig(mainYamlConfigString);
        yamlConfig->SetMainConfigVersion(NYamlConfig::GetVersion(mainYamlConfigString));
        if (storageYamlConfigString) {
            yamlConfig->SetStorageConfig(*storageYamlConfigString);
            yamlConfig->SetStorageConfigVersion(NYamlConfig::GetVersion(*storageYamlConfigString));
        }
    }

    /*
     * FIXME: if (ErrorCollector.HasFatal()) { return; }
     */

    NKikimrConfig::TAppConfig parsedConfig;

    if (storageYamlConfigString) {
        auto storage = NKikimr::NYaml::Yaml2Json(YAML::Load(*storageYamlConfigString), true);
        auto main = NKikimr::NYaml::Yaml2Json(YAML::Load(mainYamlConfigString), true);
        auto& target = main["config"].GetMapSafe();
        for (auto&& [key, value] : std::move(storage["config"].GetMapSafe())) {
            target.emplace(std::move(key), std::move(value));
        }
        NKikimr::NYaml::Parse(main, NKikimr::NYaml::GetJsonToProtoConfig(), parsedConfig, true);
    } else {
        parsedConfig = NKikimr::NYaml::Parse(mainYamlConfigString); // FIXME
    }

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

            configUpdateTracer.AddUpdate(fieldIdx, TConfigItemInfo::EUpdateKind::ReplaceConfigWithConsoleProto, location);
        }
    }
}

TString DeduceNodeDomain(const NConfig::TCommonAppOptions& cf, const NKikimrConfig::TAppConfig& appConfig) {
    if (cf.NodeDomain) {
        return cf.NodeDomain;
    }

    if (appConfig.GetDomainsConfig().DomainSize() == 1) {
        return appConfig.GetDomainsConfig().GetDomain(0).GetName();
    }

    if (appConfig.GetTenantPoolConfig().SlotsSize() == 1) {
        auto &slot = appConfig.GetTenantPoolConfig().GetSlots(0);
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

ui32 NextValidKind(ui32 kind) {
    do {
        ++kind;
        if (kind != NKikimrConsole::TConfigItem::Auto && NKikimrConsole::TConfigItem::EKind_IsValid(kind)) {
            break;
        }
    } while (kind <= NKikimrConsole::TConfigItem::EKind_MAX);
    return kind;
}

bool HasCorrespondingManagedKind(ui32 kind, const NKikimrConfig::TAppConfig& appConfig) {
    return (kind == NKikimrConsole::TConfigItem::NameserviceConfigItem && appConfig.HasNameserviceConfig()) ||
            (kind == NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem && appConfig.HasNetClassifierDistributableConfig()) ||
            (kind == NKikimrConsole::TConfigItem::NamedConfigsItem && appConfig.NamedConfigsSize());
}

NClient::TKikimr GetKikimr(const TGrpcSslSettings& cf, const TString& addr, const IEnv& env) {
    TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
    NYdbGrpc::TGRpcClientConfig grpcConfig(endpoint.Address, TDuration::Seconds(5));
    grpcConfig.LoadBalancingPolicy = "round_robin";
    if (endpoint.EnableSsl.Defined() && endpoint.EnableSsl.GetRef()) {
        grpcConfig.EnableSsl = endpoint.EnableSsl.GetRef();
        auto& sslCredentials = grpcConfig.SslCredentials;
        if (cf.PathToGrpcCaFile) {
            sslCredentials.pem_root_certs = env.ReadFromFile(cf.PathToGrpcCaFile, "CA certificates");
        }
        if (cf.PathToGrpcCertFile && cf.PathToGrpcPrivateKeyFile) {
            sslCredentials.pem_cert_chain = env.ReadFromFile(cf.PathToGrpcCertFile, "Client certificates");
            sslCredentials.pem_private_key = env.ReadFromFile(cf.PathToGrpcPrivateKeyFile, "Client certificates key");
        }
    }
    return NClient::TKikimr(grpcConfig);
}

NKikimrConfig::TAppConfig GetYamlConfigFromResult(const IConfigurationResult& result, const TMap<TString, TString>& labels) {
    NKikimrConfig::TAppConfig appConfig;
    if (result.HasMainYamlConfig() && !result.GetMainYamlConfig().empty()) {
        NYamlConfig::ResolveAndParseYamlConfig(
            result.GetMainYamlConfig(),
            result.GetVolatileYamlConfigs(),
            labels,
            appConfig,
            result.HasDatabaseYamlConfig() ? std::optional{result.GetDatabaseYamlConfig()} : std::nullopt);
    }
    return appConfig;
}

NKikimrConfig::TAppConfig GetActualDynConfig(
    const NKikimrConfig::TAppConfig& yamlConfig,
    const NKikimrConfig::TAppConfig& regularConfig,
    IConfigUpdateTracer& ConfigUpdateTracer)
{
    if (yamlConfig.GetYamlConfigEnabled()) {
        for (ui32 kind = NKikimrConsole::TConfigItem::EKind_MIN; kind <= NKikimrConsole::TConfigItem::EKind_MAX; kind = NextValidKind(kind)) {
            if (HasCorrespondingManagedKind(kind, yamlConfig)) {
                ConfigUpdateTracer.AddUpdate(kind, TConfigItemInfo::EUpdateKind::ReplaceConfigWithConsoleProto);
            } else {
                ConfigUpdateTracer.AddUpdate(kind, TConfigItemInfo::EUpdateKind::ReplaceConfigWithConsoleYaml);
            }
        }

        return yamlConfig;
    }

    for (ui32 kind = NKikimrConsole::TConfigItem::EKind_MIN; kind <= NKikimrConsole::TConfigItem::EKind_MAX; kind = NextValidKind(kind)) {
        ConfigUpdateTracer.AddUpdate(kind, TConfigItemInfo::EUpdateKind::ReplaceConfigWithConsoleProto);
    }

    return regularConfig;
}

NYdb::TDriverConfig CreateDriverConfig(const TGrpcSslSettings& grpcSettings, const TString& addr, const IEnv& env, const std::optional<TString>& authToken) {
    TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(addr);
    NYdb::TDriverConfig config;
    if (endpoint.EnableSsl.Defined() && endpoint.EnableSsl.GetRef()) {
        if (grpcSettings.PathToGrpcCaFile) {
            config.UseSecureConnection(env.ReadFromFile(grpcSettings.PathToGrpcCaFile, "CA certificates").c_str());
        }
        if (grpcSettings.PathToGrpcCertFile && grpcSettings.PathToGrpcPrivateKeyFile) {
            auto certificate = env.ReadFromFile(grpcSettings.PathToGrpcCertFile, "Client certificates");
            auto privateKey = env.ReadFromFile(grpcSettings.PathToGrpcPrivateKeyFile, "Client certificates key");
            config.UseClientCertificate(certificate.c_str(), privateKey.c_str());
        }
    }
    if (authToken) {
        config.SetAuthToken(authToken.value());
    }
    config.SetEndpoint(endpoint.Address);

    return config;
}

std::unique_ptr<IInitialConfigurator> MakeDefaultInitialConfigurator(TInitialConfiguratorDependencies deps) {
    return std::make_unique<TInitialConfiguratorImpl>(deps);
}

class TInitialConfiguratorDepsRecorder
    : public IInitialConfiguratorDepsRecorder
{
    TInitialConfiguratorDependencies Impls;
    TProtoConfigFileProviderRecorder ProtoConfigFileProvider;
    TNodeBrokerClientRecorder NodeBrokerClient;
    TDynConfigClientRecorder DynConfigClient;
    TConfigClientRecorder ConfigClient;
    TEnvRecorder Env;
public:
    TInitialConfiguratorDepsRecorder(TInitialConfiguratorDependencies deps)
        : Impls(deps)
        , ProtoConfigFileProvider(deps.ProtoConfigFileProvider)
        , NodeBrokerClient(deps.NodeBrokerClient)
        , DynConfigClient(deps.DynConfigClient)
        , ConfigClient(deps.ConfigClient)
        , Env(deps.Env)
    {}

    TInitialConfiguratorDependencies GetDeps() override {
        return TInitialConfiguratorDependencies {
            .ErrorCollector = Impls.ErrorCollector,
            .ProtoConfigFileProvider = ProtoConfigFileProvider,
            .ConfigUpdateTracer = Impls.ConfigUpdateTracer,
            .MemLogInit = Impls.MemLogInit,
            .NodeBrokerClient = NodeBrokerClient,
            .DynConfigClient = DynConfigClient,
            .ConfigClient = ConfigClient,
            .Env = Env,
            .Logger = Impls.Logger,
        };
    }

    TRecordedInitialConfiguratorDeps GetRecordedDeps() const override {
        return TRecordedInitialConfiguratorDeps {
            .ErrorCollector = MakeDefaultErrorCollector(),
            .ProtoConfigFileProvider = std::make_unique<TProtoConfigFileProviderMock>(ProtoConfigFileProvider.GetMock()),
            .ConfigUpdateTracer = MakeDefaultConfigUpdateTracer(),
            .MemLogInit = MakeNoopMemLogInitializer(),
            .NodeBrokerClient = std::make_unique<TNodeBrokerClientMock>(NodeBrokerClient.GetMock()),
            .DynConfigClient = std::make_unique<TDynConfigClientMock>(DynConfigClient.GetMock()),
            .ConfigClient = std::make_unique<TConfigClientMock>(ConfigClient.GetMock()),
            .Env = std::make_unique<TEnvMock>(Env.GetMock()),
            .Logger = MakeNoopInitLogger(),
        };
    }
};

std::unique_ptr<IInitialConfiguratorDepsRecorder> MakeDefaultInitialConfiguratorDepsRecorder(TInitialConfiguratorDependencies deps) {
    return std::make_unique<TInitialConfiguratorDepsRecorder>(deps);
}

} // namespace NKikimr::NConfig

Y_DECLARE_OUT_SPEC(, NKikimr::NConfig::TWithDefault<TString>, stream, value) {
    stream << value.Value;
}
