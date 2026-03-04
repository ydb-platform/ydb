#include "cli.h"
#include "cli_cmds.h"
#include "cli_cmds_standalone.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_service_discovery.h> // for NConsoleClient::TCommandWhoAmI
#include <ydb/core/driver_lib/run/factories.h>
#include <ydb/core/driver_lib/version/version.h>
#include <library/cpp/malloc/api/malloc.h>
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <filesystem>

namespace {

void PrintAllocatorInfoAndExit() {
    Cout << "linked with malloc: " << NMalloc::MallocInfo().Name << Endl;
    exit(0);
}

void PrintCompatibilityInfoAndExit() {
    TString compatibilityInfo(NKikimr::CompatibilityInfo.PrintHumanReadable());
    Cout << compatibilityInfo;
    if (!compatibilityInfo.EndsWith("\n")) {
        Cout << Endl;
    }
    exit(0);
}

} // anonymous namespace

namespace NKikimr {
namespace NDriverClient {

using namespace NYdb::NConsoleClient;

extern void AddClientCommandServer(TClientCommandTree& parent, std::shared_ptr<TModuleFactories> factories);

class TClientCommandRoot : public TClientCommandRootKikimrBase {
    // Dummy storage for hidden global opts consumed by "run" command.
    // The actual values are re-parsed from InitialArgV inside TCommandRun::Run().
    TString DummyClusterName;
    ui32 DummyLogLevel = 0;
    ui32 DummyLogSamplingLevel = 0;
    ui32 DummyLogSamplingRate = 0;
    TString DummyLogFormat;
    TVector<TString> DummyUDFsPaths;
    TString DummyUDFsDir;

public:
    TClientCommandRoot(const TString& name, std::shared_ptr<TModuleFactories> factories)
        : TClientCommandRootKikimrBase(name)
    {
        // Visible commands
        AddCommand(std::make_unique<TClientCommandAdmin>());
        AddCommand(std::make_unique<TClientCommandDb>());
        AddCommand(std::make_unique<TClientCommandCms>());
        AddCommand(std::make_unique<TCommandWhoAmI>());
        AddCommand(std::make_unique<TClientCommandDiscovery>());
        AddClientCommandServer(*this, factories);
        AddCommand(std::make_unique<TClientCommandConfig>());

        // Hidden commands (shown only in -hh verbose help)
        AddHiddenCommand(NewCommandRun(std::move(factories)));
        AddHiddenCommand(NewCommandFormatInfo());
        AddHiddenCommand(NewCommandFormatUtil());
        AddHiddenCommand(NewCommandNodeByHost());
        AddHiddenCommand(NewCommandSchemeInitRoot());
        AddHiddenCommand(NewCommandPersQueueRequest());
        AddHiddenCommand(NewCommandPersQueueStress());
        AddHiddenCommand(NewCommandPersQueueDiscoverClusters());
        AddHiddenCommand(NewCommandActorsysPerfTest());
    }

    void Config(TConfig& config) override {
        TClientCommandOptions& opts = *config.Opts;
        HideOptions(config.Opts->GetOpts());
        // Restore --help visibility after HideOptions hid everything
        if (auto* helpOpt = config.Opts->GetOpts().FindLongOption("help")) {
            helpOpt->Hidden_ = false;
        }
        opts.AddLongOption('k', "token", "security token").RequiredArgument("TOKEN").StoreResult(&Token);
        opts.AddLongOption('s', "server", "server address to connect")
            .RequiredArgument("HOST[:PORT]").StoreResult(&Address);

        NLastGetopt::TOpts& nOpts = config.Opts->GetOpts();
        nOpts.AddLongOption(0, "allocator-info", "Print the name of allocator linked to the binary and exit")
            .NoArgument().Handler(&PrintAllocatorInfoAndExit);
        nOpts.AddLongOption(0, "compatibility-info", "Print compatibility info of this binary and exit")
            .NoArgument().Handler(&PrintCompatibilityInfoAndExit);
        config.ExecutableOptions.insert("--allocator-info");
        config.ExecutableOptions.insert("--compatibility-info");

        // Hidden global opts for legacy "run" command.
        // These must be accepted at root level for backward compatibility:
        // "ydbd --cluster-name test run --node 1"
        // The actual values are re-parsed from InitialArgV inside TCommandRun::Run().
        nOpts.AddLongOption("cluster-name", "which cluster this node belongs to")
            .OptionalArgument("STR").StoreResult(&DummyClusterName).Hidden();
        nOpts.AddLongOption("log-level", "default logging level")
            .OptionalArgument("1-7").StoreResult(&DummyLogLevel).Hidden();
        nOpts.AddLongOption("log-sampling-level", "sample logs equal to or above this level")
            .OptionalArgument("1-7").StoreResult(&DummyLogSamplingLevel).Hidden();
        nOpts.AddLongOption("log-sampling-rate", "log only each Nth message with priority matching sampling level; 0 turns log sampling off")
            .OptionalArgument("NUM").StoreResult(&DummyLogSamplingRate).Hidden();
        nOpts.AddLongOption("log-format", "log format to use; short skips the priority and timestamp")
            .OptionalArgument("full|short|json").StoreResult(&DummyLogFormat).Hidden();
        nOpts.AddLongOption("syslog", "send to syslog instead of stderr")
            .NoArgument().Hidden();
        nOpts.AddLongOption("tcp", "start tcp interconnect")
            .NoArgument().Hidden();
        nOpts.AddLongOption("udf", "Load shared library with UDF by given path")
            .RequiredArgument("PATH").AppendTo(&DummyUDFsPaths).Hidden();
        nOpts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory")
            .RequiredArgument("PATH").StoreResult(&DummyUDFsDir).Hidden();

        TClientCommandRootKikimrBase::Config(config);
    }

    void ParseAddress(TConfig& config) override {
        if (Address.empty()) {
            TString ydbServer = GetEnv("YDB_SERVER");
            if (ydbServer == nullptr) {
                ydbServer = GetEnv("KIKIMR_SERVER");
            }
            if (ydbServer != nullptr) {
                Address = ydbServer;
            }
        }
        config.Address = Address;

        TCommandConfig::TServerEndpoint endpoint = TCommandConfig::ParseServerAddress(Address);
        if (endpoint.EnableSsl.Defined()) {
            config.EnableSsl = endpoint.EnableSsl.GetRef();
        }
        ParseCaCerts(config);
        ParseClientCert(config);

        CommandConfig.ClientConfig = NYdbGrpc::TGRpcClientConfig(endpoint.Address);
        if (config.EnableSsl) {
            CommandConfig.ClientConfig.EnableSsl = config.EnableSsl;
            CommandConfig.ClientConfig.SslCredentials.pem_root_certs = config.CaCerts;
            if (config.ClientCert) {
                CommandConfig.ClientConfig.SslCredentials.pem_cert_chain = config.ClientCert;
                CommandConfig.ClientConfig.SslCredentials.pem_private_key = config.ClientCertPrivateKey;
            }
        }
    }
};

int NewClient(int argc, char** argv, std::shared_ptr<TModuleFactories> factories) {
    auto commandsRoot = MakeHolder<TClientCommandRoot>(std::filesystem::path(argv[0]).stem().string(), std::move(factories));
    TClientCommand::TConfig config(argc, argv);
    // TODO: process flags from environment KIKIMR_FLAGS before command line processing
    return commandsRoot->Process(config);
}

}
}
