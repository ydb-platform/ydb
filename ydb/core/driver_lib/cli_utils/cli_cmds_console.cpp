#include "cli.h"
#include "cli_cmds.h"

#include <ydb/library/yaml_config/console_dumper.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/core/protos/console.pb.h>

#include <util/string/type.h>
#include <util/string/split.h>
#include <util/system/fs.h>

namespace NKikimr {
namespace NDriverClient {

class TConsoleClientCommand : public TClientCommandBase {
public:
    TString Domain;
    ui32 Retries;
    NKikimrClient::TConsoleRequest Request;

    TConsoleClientCommand(const TString &name,
                         const std::initializer_list<TString> &aliases,
                         const TString &description)
        : TClientCommandBase(name, aliases, description)
        , Retries(0)
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommand::Config(config);

        Domain = "";

        config.Opts->AddLongOption("domain", "Set target domain (required for clusters with multiple domains)")
            .RequiredArgument("NAME").StoreResult(&Domain);
        config.Opts->AddLongOption("retry", "Set how many times retriable statuses should be retried")
            .RequiredArgument("COUNT").StoreResult(&Retries);
    }

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);

        if (Domain)
            Request.SetDomainName(Domain);
    }

    int Run(TConfig &config) override
    {
        bool retry = false;
        auto handler = [this, &retry](const NMsgBusProxy::TBusConsoleResponse &response) -> int {
            auto status = response.Record.GetStatus().GetCode();
            if ((status == Ydb::StatusIds::UNAVAILABLE
                 || status == Ydb::StatusIds::TIMEOUT)
                && Retries) {
                TConsoleClientCommand::PrintResponse(response.Record);
                --Retries;
                retry = true;

                TString s = (Retries == 1 ? "" : "s");
                TString ss = (Retries == 1 ? "s" : "");
                Cout << "Retrying... (" << Retries << " attempt" << s
                     << " remain" << ss << ")" << Endl;

                return -1;
            }
            retry = false;
            PrintResponse(response.Record);
            return status == Ydb::StatusIds::SUCCESS ? 0 : -1;
        };

        int result;
        do {
            TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest);
            request->Record.CopyFrom(Request);
            result = MessageBusCall<NMsgBusProxy::TBusConsoleRequest,
                                    NMsgBusProxy::TBusConsoleResponse>(config, request, handler);
        } while (retry);

        return result;
    }

    virtual void PrintResponse(const NKikimrClient::TConsoleResponse &response)
    {
        if (response.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS)
            Cout << response.DebugString();
        else
            Cout << "ERROR: " << response.GetStatus().GetCode()
                 << " (" << response.GetStatus().GetReason() << ")" << Endl;
    }

    void ParseStoragePools(TConfig &config, TVector<std::pair<TString, ui32>> &pools)
    {
        for (auto &arg : config.ParseResult->GetFreeArgs()) {
            TVector<TString> items = StringSplitter(arg).Split(':').ToList<TString>();
            if (items.size() != 2)
                ythrow yexception() << "bad format in '" + arg + "'";
            ui32 size = FromString<ui32>(items[1]);
            if (!size)
                ythrow yexception() << "bad pool size '" + arg + "'";
            pools.push_back(std::make_pair(items[0], size));
        }
    }
};

class TConfigWriter {
public:
    TConfigWriter()
    {
        ConfigFiles["ActorSystemConfig"] = "sys.txt";
        ConfigFiles["LogConfig"] = "log.txt";
        ConfigFiles["NameserviceConfig"] = "names.txt";
        ConfigFiles["InterconnectConfig"] = "ic.txt";
        ConfigFiles["DomainsConfig"] = "domains.txt";
        ConfigFiles["BlobStorageConfig"] = "bs.txt";
        ConfigFiles["ChannelProfileConfig"] = "channels.txt";
        ConfigFiles["BootstrapConfig"] = "boot.txt";
        ConfigFiles["VDiskConfig"] = "vdisks.txt";
        ConfigFiles["KQPConfig"] = "kqp.txt";
        ConfigFiles["GRpcConfig"] = "grpc.txt";
        ConfigFiles["CmsConfig"] = "cms.txt";
        ConfigFiles["FeatureFlags"] = "feature_flags.txt";
        ConfigFiles["SqsConfig"] = "sqs.txt";
        ConfigFiles["PQConfig"] = "pq.txt";
        ConfigFiles["PQClusterDiscoveryConfig"] = "pqcd.txt";
        ConfigFiles["NetClassifierConfig"] = "netclassifier.txt";
        ConfigFiles["KeyConfig"] = "key.txt";
        ConfigFiles["PDiskKeyConfig"] = "pdisk_key.txt";
        ConfigFiles["ClusterYamlConfig"] = "cluster.yaml";
        ConfigFiles["HttpProxy"] = "http_proxy.txt";

    }

    void Write(const TString &outDir,
               NKikimrConfig::TAppConfig config)
    {
        NFs::RemoveRecursive(outDir);

        if (!NFs::MakeDirectoryRecursive(outDir))
            ythrow yexception() << "Cannot create directory " << outDir << Endl;

        TUnbufferedFileOutput ffs(outDir + "/full.txt");
        ffs << config.Utf8DebugString();

        auto *desc = config.GetDescriptor();
        auto *reflection = config.GetReflection();
        std::vector<const NProtoBuf::FieldDescriptor *> otherFields;
        for (int i = 0; i < desc->field_count(); ++i) {
            auto *field = desc->field(i);
            TString fname;

            if (ConfigFiles.contains(field->name()))
                fname = ConfigFiles.at(field->name());
            else
                fname = field->name() + ".txt";

            if (field->is_repeated()) {
                if (reflection->FieldSize(config, field)) {
                    TUnbufferedFileOutput fs(outDir + "/" + fname);
                    NKikimrConfig::TAppConfig cfg;
                    reflection->SwapFields(&config, &cfg, {field});
                    fs << cfg.Utf8DebugString();
                }
            } else if (reflection->HasField(config, field)) {
                TUnbufferedFileOutput fs(outDir + "/" + fname);
                if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_MESSAGE)
                    fs << reflection->MutableMessage(&config, field)->Utf8DebugString();
                else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_STRING)
                    fs << reflection->GetString(config, field);
                else {
                    NKikimrConfig::TAppConfig cfg;
                    reflection->SwapFields(&config, &cfg, {field});
                    fs << cfg.Utf8DebugString();
                }
            }
        }
    }

private:
    THashMap<TString, TString> ConfigFiles;
};

class TClientCommandConsoleConfigsUpdate : public TConsoleClientCommand {
    bool DryRun;
    TString OutDir;

public:
    TClientCommandConsoleConfigsUpdate()
        : TConsoleClientCommand("update", {}, "Execute Console configure request")
        , DryRun(false)
    {
    }

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.Opts->AddLongOption("dry-run", "Execute configure request in dry-run mode")
            .NoArgument().SetFlag(&DryRun);
        config.Opts->AddLongOption("out-dir", "Output affected configs into specified directory")
            .RequiredArgument("PATH").StoreResult(&OutDir);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<CONFIGURE-PROTO>", "Console configure request protobuf or file with protobuf");
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        ParseProtobuf(Request.MutableConfigureRequest(), config.ParseResult->GetFreeArgs()[0]);
        if (DryRun)
            Request.MutableConfigureRequest()->SetDryRun(DryRun);
        if (OutDir)
            Request.MutableConfigureRequest()->SetFillAffectedConfigs(true);
    }

    virtual void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Cout << "ERROR: " << response.GetStatus().GetCode()
                 << " (" << response.GetStatus().GetReason() << ")" << Endl;
            return;
        }

        Cout << "OK" << Endl;

        if (OutDir)
            NFs::RemoveRecursive(OutDir);

        auto &resp = response.GetConfigureResponse();
        if (resp.AffectedConfigsSize()) {
            Cout << "Affected configs:" << Endl;
            for (auto &entry : resp.GetAffectedConfigs()) {
                TString dirName;

                Cout << "  ";
                if (entry.GetTenant()) {
                    Cout << "Tenant: '" << entry.GetTenant() << "' ";
                    dirName = entry.GetTenant();
                }
                if (entry.GetNodeType()) {
                    Cout << "Node type: '" << entry.GetNodeType() << "' ";
                    if (dirName)
                        dirName += "-";
                    dirName += entry.GetNodeType();
                }
                if (!entry.GetTenant() && !entry.GetNodeType()) {
                    Cout << "Domain";
                    dirName = "domain";
                }
                Cout << Endl;

                if (OutDir) {
                    TConfigWriter writer;
                    writer.Write(OutDir + "/" + dirName + "/current", entry.GetOldConfig());
                    writer.Write(OutDir + "/" + dirName + "/modified", entry.GetNewConfig());
                }
            }
        }
    }
};

class TClientCommandConsoleConfigsLoad : public TConsoleClientCommand {
    TString Tenant;
    TString NodeType;
    TString Host;
    ui32 NodeId;
    TString OutDir;
    TString Kind;

public:
    TClientCommandConsoleConfigsLoad()
        : TConsoleClientCommand("load", {}, "Load config from CMS for node with specified attributes")
        , NodeId(0)
    {
    }

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.Opts->AddLongOption("out-dir", "Output directory for configs")
            .RequiredArgument("PATH").StoreResult(&OutDir).Required();
        config.Opts->AddLongOption("tenant", "Tenant attribute for requested config")
            .RequiredArgument("NAME").StoreResult(&Tenant);
        config.Opts->AddLongOption("node-type", "Node type attribute for requested config")
            .RequiredArgument("TYPE").StoreResult(&NodeType);
        config.Opts->AddLongOption("node-id", "Tenant attribute for requested config")
            .RequiredArgument("ID").StoreResult(&NodeId);
        config.Opts->AddLongOption("host", "Host attribute for requested config")
            .RequiredArgument("FQDN").StoreResult(&Host);
        config.SetFreeArgsNum(0);
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        auto &rec = *Request.MutableGetNodeConfigRequest();
        if (Tenant)
            rec.MutableNode()->SetTenant(Tenant);
        if (NodeType)
            rec.MutableNode()->SetNodeType(NodeType);
        if (NodeId)
            rec.MutableNode()->SetNodeId(NodeId);
        if (Host)
            rec.MutableNode()->SetHost(Host);
    }

    virtual void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Cout << "ERROR: " << response.GetStatus().GetCode()
                 << " (" << response.GetStatus().GetReason() << ")" << Endl;
            return;
        }

        Cout << "OK" << Endl;

        TConfigWriter writer;
        writer.Write(OutDir, response.GetGetNodeConfigResponse().GetConfig());
    }
};

class TClientCommandConsoleConfigsDumpYaml : public TConsoleClientCommand {
public:
    TClientCommandConsoleConfigsDumpYaml()
        : TConsoleClientCommand("dump-yaml", {}, "Dump config in yaml format")
    {
    }

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.SetFreeArgsNum(0);
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);
        Request.MutableGetConfigItemsRequest();
    }

    virtual void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Cout << "ERROR: " << response.GetStatus().GetCode()
                 << " (" << response.GetStatus().GetReason() << ")" << Endl;
            return;
        }

        auto &items = response.GetGetConfigItemsResponse().GetConfigItems();

        Cout << NYamlConfig::DumpConsoleConfigs(items);
    }
};

class TClientCommandConvertToYaml: public TClientCommandBase {
    NKikimrConsole::TConfigureRequest Request;
public:
    TClientCommandConvertToYaml()
        : TClientCommandBase("convert-to-yaml", {}, "Convert config-item to yaml format")
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<CONFIGURE-PROTO>", "Console configure request protobuf or file with protobuf");
    }

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);
        ParseProtobuf(&Request, config.ParseResult->GetFreeArgs()[0]);
    }

    int Run(TConfig &) override
    {
        bool domain = false;

        TStringStream result;
        TStringStream warnings;

        for (auto& action : Request.GetActions()) {
            if (action.HasModifyConfigItem()) {
                Cerr << "Error: Modify items are not supported" << Endl;
                return 1;
            }

            if (action.HasRemoveConfigItem()) {
                warnings << "Warning: this config contains remove, you should remove entry with id="
                         << action.GetRemoveConfigItem().GetConfigItemId().GetId() << "." << action.GetRemoveConfigItem().GetConfigItemId().GetGeneration()
                         << " by hand" << Endl;
                continue;
            }

            if (action.HasRemoveConfigItems()) {
                warnings << "Warning: this config contains remove, you should remove entry with cookie=";
                for (auto& cookie : action.GetRemoveConfigItems().GetCookieFilter().GetCookies()) {
                    warnings << "\"" << cookie << "\",";
                }
                warnings << " by hand" << Endl;
                continue;
            }

            auto add = action.GetAddConfigItem().GetConfigItem();

            auto [hasDomain, cfg] =  NYamlConfig::DumpConsoleConfigItem(add);

            domain |= hasDomain;

            result << cfg << Endl;
        }

        if (domain) {
            warnings << "Warning: this config contains domain config item, it should be merged by hand or inserted before scoped selectors" << Endl;
        }

        Cerr << warnings.Str();
        Cout << result.Str();

        return 0;
    }
};

class TClientCommandConvertFromYaml: public TClientCommandBase {
    TString Request;
    TString Domain;
public:
    TClientCommandConvertFromYaml()
        : TClientCommandBase("convert-from-yaml", {}, "Convert config-item from yaml format")
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommand::Config(config);
        config.Opts->AddLongOption("domain", "domain where config will be applied")
            .RequiredArgument("<DOMAIN>").StoreResult(&Domain).Required();
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<CONFIGURE-YAML>", "Console configure request yaml");
    }

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);

        Request = TUnbufferedFileInput(config.ParseResult->GetFreeArgs()[0]).ReadAll();
    }

    int Run(TConfig &) override
    {
        NKikimrConsole::TConfigureRequest req = NYamlConfig::DumpYamlConfigRequest(Request, Domain);
        TString result;
        google::protobuf::TextFormat::PrintToString(req, &result);
        Cout << result;

        return 0;
    }
};

class TClientCommandConsoleConfigs : public TClientCommandTree {
public:
    TClientCommandConsoleConfigs()
        : TClientCommandTree("configs", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandConsoleConfigsLoad>());
        AddCommand(std::make_unique<TClientCommandConsoleConfigsDumpYaml>());
        AddCommand(std::make_unique<TClientCommandConvertToYaml>());
        AddCommand(std::make_unique<TClientCommandConvertFromYaml>());
        AddCommand(std::make_unique<TClientCommandConsoleConfigsUpdate>());
    }
};

class TClientCommandConsoleExecute : public TConsoleClientCommand {
public:
    TClientCommandConsoleExecute()
        : TConsoleClientCommand("execute", { "exec" }, "Execute console request protobuf")
    {}

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<SCHEMA-PROTO>", "Console request protobuf or file with protobuf");
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        ParseProtobuf(&Request, config.ParseResult->GetFreeArgs()[0]);
    }
};

class TClientCommandConsoleConfigGet : public TConsoleClientCommand {
public:
    TClientCommandConsoleConfigGet()
        : TConsoleClientCommand("get", {}, "Get current Console config")
    {}

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);
        Request.MutableGetConfigRequest();
    }

    void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TConsoleClientCommand::PrintResponse(response);
        } else {
            Cout << "Curent Console config: " << Endl
                 << response.GetGetConfigResponse().GetConfig().DebugString();
        }
    }
};

class TClientCommandConsoleConfigSet : public TConsoleClientCommand {
private:
    bool Merge = false;
    bool MergeOverwriteRepeated = false;

public:
    TClientCommandConsoleConfigSet()
        : TConsoleClientCommand("set", {}, "Modify Console config")
    {}

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.Opts->AddLongOption("merge", "Merge provided config with the current one")
            .NoArgument().SetFlag(&Merge);
        config.Opts->AddLongOption("merge-overwrite-repeated", "Merge provided config with the current one"
                                   " but overwrite those repeated field which are not empty in provided config")
            .NoArgument().SetFlag(&MergeOverwriteRepeated);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<CONFIG-PROTO>", "Console config protobuf or file with protobuf");
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        ParseProtobuf(Request.MutableSetConfigRequest()->MutableConfig(),
                      config.ParseResult->GetFreeArgs()[0]);

        if (Merge && MergeOverwriteRepeated)
            ythrow yexception() << "can't apply both --merge and --merge-overwrite-repeated";

        if (Merge)
            Request.MutableSetConfigRequest()->SetMerge(NKikimrConsole::TConfigItem::MERGE);
        else if (MergeOverwriteRepeated)
            Request.MutableSetConfigRequest()->SetMerge(NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED);
    }

    void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TConsoleClientCommand::PrintResponse(response);
        } else {
            Cout << "OK" << Endl;
        }
    }
};

class TClientCommandConsoleConfigTree : public TClientCommandTree {
public:
    TClientCommandConsoleConfigTree()
        : TClientCommandTree("config", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandConsoleConfigGet>());
        AddCommand(std::make_unique<TClientCommandConsoleConfigSet>());
    }
};

class TClientCommandConsoleToggleValidator : public TConsoleClientCommand {
private:
    bool Disable;

public:
    TClientCommandConsoleToggleValidator(const TString &name,
                                         const TString &descr,
                                         bool disable)
        : TConsoleClientCommand(name, {}, descr)
        , Disable(disable)
    {}

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
        config.SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<NAME>", "Config validator name");
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        auto &rec = *Request.MutableToggleConfigValidatorRequest();
        rec.SetName(config.ParseResult->GetFreeArgs()[0]);
        rec.SetDisable(Disable);
    }

    void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TConsoleClientCommand::PrintResponse(response);
        } else {
            Cout << "OK" << Endl;
        }
    }
};

class TClientCommandConsoleValidatorEnable : public TClientCommandConsoleToggleValidator {
private:

public:
    TClientCommandConsoleValidatorEnable()
        : TClientCommandConsoleToggleValidator("enable", "Enable config validator", false)
    {}
};

class TClientCommandConsoleValidatorDisable : public TClientCommandConsoleToggleValidator {
private:

public:
    TClientCommandConsoleValidatorDisable()
        : TClientCommandConsoleToggleValidator("disable", "Disable config validator", true)
    {}
};

class TClientCommandConsoleValidatorList : public TConsoleClientCommand {
private:

public:
    TClientCommandConsoleValidatorList()
        : TConsoleClientCommand("list", {}, "List config validators")
    {}

    virtual void Config(TConfig& config) override {
        TConsoleClientCommand::Config(config);
    }

    virtual void Parse(TConfig& config) override {
        TConsoleClientCommand::Parse(config);

        Request.MutableListConfigValidatorsRequest();
    }

    void PrintResponse(const NKikimrClient::TConsoleResponse &response) override
    {
        if (response.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TConsoleClientCommand::PrintResponse(response);
        } else {
            for (auto &rec : response.GetListConfigValidatorsResponse().GetValidators()) {
                Cout << "- " << rec.GetName()
                     << (rec.GetEnabled() ? " ENABLED" : " DISABLED") << Endl
                     << "  " << rec.GetDescription() << Endl
                     << "  CheckedItems:";

                if (rec.CheckedItemKindsSize()) {
                    for (auto &kind : rec.GetCheckedItemKinds())
                        Cout << " " << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
                } else {
                    Cout << " ALL";
                }
                Cout << Endl;
            }
        }
    }
};

class TClientCommandConsoleValidator : public TClientCommandTree {
public:
    TClientCommandConsoleValidator()
        : TClientCommandTree("validator", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandConsoleValidatorDisable>());
        AddCommand(std::make_unique<TClientCommandConsoleValidatorEnable>());
        AddCommand(std::make_unique<TClientCommandConsoleValidatorList>());
    }
};

TClientCommandConsole::TClientCommandConsole()
    : TClientCommandTree("console", {}, "Console commands")
{
    AddCommand(std::make_unique<TClientCommandConsoleConfigTree>());
    AddCommand(std::make_unique<TClientCommandConsoleConfigs>());
    AddCommand(std::make_unique<TClientCommandConsoleExecute>());
    AddCommand(std::make_unique<TClientCommandConsoleValidator>());
}

}
}
