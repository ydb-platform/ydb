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
