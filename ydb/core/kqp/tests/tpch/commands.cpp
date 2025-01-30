#include "commands.h"

#include "cmd_drop.h"
#include "cmd_prepare.h"
#include "cmd_prepare_scheme.h"
#include "cmd_run_query.h"
#include "cmd_run_bench.h"

#include <util/system/env.h>


namespace NYdb::NTpch {

using namespace NConsoleClient;

TClientCommandTpchRoot::TClientCommandTpchRoot()
    : TClientCommandRootBase("tpch")
{
    AddCommand(std::make_unique<TCommandDrop>());
    AddCommand(std::make_unique<TCommandPrepare>());
    AddCommand(std::make_unique<TCommandPrepareScheme>());
    AddCommand(std::make_unique<TCommandRunQuery>());
    AddCommand(std::make_unique<TCommandRunBenchmark>());
}

void TClientCommandTpchRoot::Config(TConfig& config) {
    config.Opts->AddLongOption('e', "endpoint", "Endpoint to connect")
        .Required()
        .StoreResult(&Address);
    config.Opts->AddLongOption('d', "database", "TablesPath to work with")
        .Required()
        .StoreResult(&Database);
    config.Opts->AddLongOption('p', "path", "Path to TPC-H tables (relative)")
        .Required()
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            if (arg.StartsWith('/')) {
                ythrow NLastGetopt::TUsageException() << "Path must be relative";
            }
            Path = arg;
        });
    TClientCommandRootBase::Config(config);

    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << " [options...] <subcommand>" << Endl << Endl
           << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandsDescription(stream, colors);
    config.Opts->SetCmdLineDescr(stream.Str());
}

void TClientCommandTpchRoot::Parse(TConfig& config) {
    TClientCommandRootBase::Parse(config);
    config.Database = Database;

    if (auto token = GetEnv("YDB_TOKEN")) {
        config.SecurityToken = token;
    }
}

void TClientCommandTpchRoot::ParseAddress(TConfig& config) {
    config.Address = Address;
}

void TClientCommandTpchRoot::Validate(TConfig& config) {
    if (Database.empty() && config.NeedToConnect) {
        throw TMisuseException() << "Missing required option 'database'.";
    }
    if (Address.empty() && config.NeedToConnect) {
        throw TMisuseException() << "Missing required option 'endpoint'.";
    }
}

int TClientCommandTpchRoot::Run(TConfig& config) {
    if (SelectedCommand) {
        dynamic_cast<TTpchCommandBase*>(SelectedCommand)->SetPath(TString::Join(Database, '/', Path));
    }
    return TClientCommandRootBase::Run(config);
}

int NewTpchClient(int argc, char** argv) {
    THolder<TClientCommandTpchRoot> cmds = MakeHolder<TClientCommandTpchRoot>();
    cmds->Opts.SetTitle("TPC-H tool");
    TClientCommand::TConfig config(argc, argv);
    return cmds->Process(config);
}

} // NYdb::NTpch
