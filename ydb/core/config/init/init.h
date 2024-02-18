#pragma once

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/core/base/location.h>
#include <ydb/core/driver_lib/run/config.h>
#include <ydb/core/cms/console/config_item_info.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>

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

namespace fs = std::filesystem;

extern TAutoPtr<NKikimrConfig::TActorSystemConfig> DummyActorSystemConfig();
extern TAutoPtr<NKikimrConfig::TAllocatorConfig> DummyAllocatorConfig();

namespace NKikimr::NConfig {

constexpr TStringBuf NODE_KIND_YDB = "ydb";
constexpr TStringBuf NODE_KIND_YQ = "yq";

constexpr static ui32 DefaultLogLevel = NActors::NLog::PRI_WARN; // log settings
constexpr static ui32 DefaultLogSamplingLevel = NActors::NLog::PRI_DEBUG; // log settings
constexpr static ui32 DefaultLogSamplingRate = 0; // log settings

struct TCallContext {
    const char* File;
    int Line;
};

#define TRACE_CONFIG_CHANGE(CHANGE_CONTEXT, KIND, CHANGE_KIND) \
    ConfigUpdateTracer->Add(KIND, TConfigItemInfo::TUpdate{CHANGE_CONTEXT.File, static_cast<ui32>(CHANGE_CONTEXT.Line), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define TRACE_CONFIG_CHANGE_INPLACE(KIND, CHANGE_KIND) \
    ConfigUpdateTracer->Add(KIND, TConfigItemInfo::TUpdate{__FILE__, static_cast<ui32>(__LINE__), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define TRACE_CONFIG_CHANGE_INPLACE_T(KIND, CHANGE_KIND) \
    ConfigUpdateTracer->Add(NKikimrConsole::TConfigItem:: KIND ## Item, TConfigItemInfo::TUpdate{__FILE__, static_cast<ui32>(__LINE__), TConfigItemInfo::EUpdateKind:: CHANGE_KIND})

#define CALL_CTX() ::NKikimr::NConfig::TCallContext{__FILE__, __LINE__}

template<typename T>
bool ParsePBFromString(const TString &content, T *pb, bool allowUnknown = false) {
    if (!allowUnknown) {
        return ::google::protobuf::TextFormat::ParseFromString(content, pb);
    }

    ::google::protobuf::TextFormat::Parser parser;
    parser.AllowUnknownField(true);
    return parser.ParseFromString(content, pb);
}

class IEnv {
public:
    virtual ~IEnv() {}
    virtual TString HostName() const = 0;
    virtual TString FQDNHostName() const = 0;
};

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
};

class IErrorCollector {
public:
    virtual ~IErrorCollector() {}
    virtual void Fatal(TString error) = 0;
};

class TDefaultErrorCollector
    : public IErrorCollector
{
public:
    void Fatal(TString error) override {
        Cerr << error << Endl;
    }
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

class IConfigUpdateTracer {
public:
    virtual ~IConfigUpdateTracer() {}
    virtual void Add(ui32 kind, TConfigItemInfo::TUpdate) = 0;
    virtual THashMap<ui32, TConfigItemInfo> Dump() const = 0;
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

    IConfigUpdateTracer* ConfigUpdateTracer = &refs.Tracer;
    auto& errorCollector = refs.ErrorCollector;
    auto& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    if ((appConfig.*hasConfig)()) {
        return nullptr; // this field is already provided in AppConfig, so we don't overwrite it
    }

    if (optname && protoConfigFileProvider.Has(optname)) {
        auto *res = (appConfig.*mutableConfig)();

        TString path = protoConfigFileProvider.Get(optname);
        const TString protoString = protoConfigFileProvider.GetProtoFromFile(path, errorCollector);
        /*
         * FIXME: if (ErrorCollector.HasFatal()) { return; }
         */
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

    IConfigUpdateTracer* ConfigUpdateTracer = &refs.Tracer;
    auto& errorCollector = refs.ErrorCollector;
    auto& protoConfigFileProvider = refs.ProtoConfigFileProvider;

    if (protoConfigFileProvider.Has(optname)) {
        typename std::remove_reference<decltype(*(appConfig.*mutableConfig)())>::type cfg;

        TString path = protoConfigFileProvider.Get(optname);
        const TString protoString = protoConfigFileProvider.GetProtoFromFile(path, errorCollector);
        /*
         * FIXME: if (ErrorCollector.HasFatal()) { return; }
         */
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

    IConfigUpdateTracer* ConfigUpdateTracer = &refs.Tracer;
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

struct TConfigFields {
    TMaybe<ui32> LogLevel; // log settings
    TMaybe<ui32> LogSamplingLevel; // log settings
    TMaybe<ui32> LogSamplingRate; // log settings
    TMaybe<TString> LogFormat;// log settings
    TMaybe<TString> SysLogServiceTag; //unique tags for sys logs
    TMaybe<TString> LogFileName; // log file name to initialize file log backend
    TMaybe<TString> ClusterName; // log settings

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
    bool IgnoreCmsConfigs = false;
    bool TinyMode = false;
    TString NodeAddress;
    TString NodeHost;
    TString NodeResolveHost;
    TString NodeDomain;
    ui32 InterconnectPort = 0;
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
    TString PathToGrpcCertFile;
    TString PathToInterconnectCertFile;
    TString PathToGrpcPrivateKeyFile;
    TString PathToInterconnectPrivateKeyFile;
    TString PathToGrpcCaFile;
    TString PathToInterconnectCaFile;
    TString YamlConfigFile;
    bool SysLogEnabled = false;
    bool TcpEnabled = false;
    bool SuppressVersionCheck = false;

    void RegisterCliOptions(NLastGetopt::TOpts& opts) {
        // FIXME remove default value where TMaybe used
        opts.AddLongOption("cluster-name", "which cluster this node belongs to")
            .DefaultValue("unknown").OptionalArgument("STR").StoreResult(&ClusterName);
        opts.AddLongOption("log-level", "default logging level").OptionalArgument("1-7")
            .DefaultValue(ToString(DefaultLogLevel)).StoreResult(&LogLevel);
        opts.AddLongOption("log-sampling-level", "sample logs equal to or above this level").OptionalArgument("1-7")
            .DefaultValue(ToString(DefaultLogSamplingLevel)).StoreResult(&LogSamplingLevel);
        opts.AddLongOption("log-sampling-rate",
                           "log only each Nth message with priority matching sampling level; 0 turns log sampling off")
            .OptionalArgument(Sprintf("0,%" PRIu32, Max<ui32>()))
            .DefaultValue(ToString(DefaultLogSamplingRate)).StoreResult(&LogSamplingRate);
        opts.AddLongOption("log-format", "log format to use; short skips the priority and timestamp")
            .DefaultValue("full").OptionalArgument("full|short|json").StoreResult(&LogFormat);
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
                .OptionalArgument("PATH").DefaultValue(RestartsCountFile).StoreResult(&RestartsCountFile);
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
        opts.AddLongOption("grpc-cert", "Path to client certificate file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcCertFile);
        opts.AddLongOption("ic-cert", "Path to client certificate file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCertFile);
        opts.AddLongOption("key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        opts.AddLongOption("grpc-key", "Path to private key file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcPrivateKeyFile);
        opts.AddLongOption("ic-key", "Path to private key file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectPrivateKeyFile);
        opts.AddLongOption("ca", "Path to certificate authority file (PEM) for interconnect").RequiredArgument("PATH").StoreResult(&PathToInterconnectCaFile);
        opts.AddLongOption("grpc-ca", "Path to certificate authority file (PEM) for grpc").RequiredArgument("PATH").StoreResult(&PathToGrpcCaFile);
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

    void ApplyFields(NKikimrConfig::TAppConfig& appConfig, IEnv& env, IConfigUpdateTracer* ConfigUpdateTracer) const {
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

        if (!PathToGrpcCertFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToCertificateFile(PathToGrpcCertFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().HasKey()) {
            appConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(appConfig.GetGRpcConfig().GetKey());
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!PathToGrpcPrivateKeyFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToPrivateKeyFile(PathToGrpcPrivateKeyFile);
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().HasCA()) {
            appConfig.MutableGRpcConfig()->SetPathToCaFile(appConfig.GetGRpcConfig().GetCA());
            TRACE_CONFIG_CHANGE_INPLACE_T(GRpcConfig, UpdateExplicitly);
        }

        if (!PathToGrpcCaFile.Empty()) {
            appConfig.MutableGRpcConfig()->SetPathToCaFile(PathToGrpcCaFile);
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

    NActors::TNodeLocation CreateNodeLocation() const { // FIXME
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

    void ApplyLogSettings(NKikimrConfig::TAppConfig& appConfig, IConfigUpdateTracer* ConfigUpdateTracer) {
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

    TString HostAndICPort(IEnv& env) const { // FIXME
        try {
            auto hostname = to_lower(env.HostName());
            hostname = hostname.substr(0, hostname.find('.'));
            return TStringBuilder() << hostname << ":" << InterconnectPort;
        } catch (TSystemError& error) {
            return "";
        }
    }

    void SetupLogConfigDefaults(NKikimrConfig::TLogConfig& logConfig, IConfigUpdateTracer* ConfigUpdateTracer) const {
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

    void SetupBootstrapConfigDefaults(NKikimrConfig::TBootstrap& bootstrapConfig, IConfigUpdateTracer* ConfigUpdateTracer) const {
        bootstrapConfig.MutableCompileServiceConfig()->SetInflightLimit(CompileInflightLimit);
        TRACE_CONFIG_CHANGE_INPLACE_T(BootstrapConfig, UpdateExplicitly);
    };

    void SetupInterconnectConfigDefaults(NKikimrConfig::TInterconnectConfig& icConfig, IConfigUpdateTracer* ConfigUpdateTracer) const {
        if (TcpEnabled) {
            icConfig.SetStartTcp(true);
            TRACE_CONFIG_CHANGE_INPLACE_T(InterconnectConfig, UpdateExplicitly);
        }
    };

    NYdb::NDiscovery::TNodeRegistrationSettings GetNodeRegistrationSettings(
            const TString &domainName,
            const TString &nodeHost,
            const TString &nodeAddress,
            const TString &nodeResolveHost,
            const TMaybe<TString>& path) const
    {
        NYdb::NDiscovery::TNodeRegistrationSettings settings;
        settings.Host(nodeHost);
        settings.Port(InterconnectPort);
        settings.ResolveHost(nodeResolveHost);
        settings.Address(nodeAddress);
        settings.DomainPath(domainName);
        settings.FixedNodeId(FixedNodeID);
        if (path) {
            settings.Path(*path);
        }

        auto loc = CreateNodeLocation();
        NActorsInterconnect::TNodeLocation tmpLocation;
        loc.Serialize(&tmpLocation, false);

        NYdb::NDiscovery::TNodeLocation settingLocation;
        CopyNodeLocation(&settingLocation, tmpLocation);
        settings.Location(settingLocation);
        return settings;
    }

    void FillClusterEndpoints(const NKikimrConfig::TAppConfig& appConfig, TVector<TString> &addrs) const {
        if (!NodeBrokerAddresses.empty()) {
            for (auto addr: NodeBrokerAddresses) {
                addrs.push_back(addr);
            }
        } else {
            Y_ABORT_UNLESS(NodeBrokerPort);
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

struct TMbusConfigFields {
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

    void InitMessageBusConfig(NKikimrConfig::TAppConfig& appConfig) {
        auto messageBusConfig = appConfig.MutableMessageBusConfig();
        messageBusConfig->SetStartBusProxy(Start);
        messageBusConfig->SetBusProxyPort(BusProxyPort);

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
};

} // namespace NKikimr::NConfig
