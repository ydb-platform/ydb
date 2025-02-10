#include "ydb_admin.h"

#include "ydb_dynamic_config.h"
#include "ydb_storage_config.h"
#include "ydb_cluster.h"

#include <ydb/public/lib/ydb_cli/common/command_utils.h>

namespace NYdb {
namespace NConsoleClient {

using namespace NUtils;

class TCommandNode : public TClientCommandTree {
public:
    TCommandNode()
        : TClientCommandTree("node", {}, "Node-wide administration")
    {}
};

class TCommandDatabase : public TClientCommandTree {
public:
    TCommandDatabase()
        : TClientCommandTree("database", {}, "Database-wide administration")
    {
        AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>());
    }
};

TCommandAdmin::TCommandAdmin()
    : TClientCommandTree("admin", {}, "Administrative cluster operations")
{
    MarkDangerous();
    UseOnlyExplicitProfile();
    // keep old commands "safe", to keep old behavior
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandConfig>(
                         NDynamicConfig::TCommandFlagsOverrides{.Dangerous = false, .OnlyExplicitProfile = false},
                         false));
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandVolatileConfig>());
    AddHiddenCommand(std::make_unique<NStorageConfig::TCommandStorageConfig>(false));
    AddCommand(std::make_unique<NCluster::TCommandCluster>());
    AddCommand(std::make_unique<TCommandNode>());
    AddCommand(std::make_unique<TCommandDatabase>());
}

void TCommandAdmin::Config(TConfig& config) {
    TClientCommand::Config(config);
    SetFreeArgs(config);
    TString commands;
    SetFreeArgTitle(0, "<subcommand>", commands);
    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << Endl << Endl
           << colors.BoldColor()
           << "Commands in this subtree may damage your cluster if used wrong" << Endl
           << "Due to dangerous nature of this commands ALL global parameters must be set explicitly" << Endl
           << "Profiles are disabled by default, and used only if set explicitly (--profile <profile-name>)" << Endl
           << "Some commands do not require global options that are required otherwise"
           << colors.OldColor();
    stream << Endl << Endl
        << colors.BoldColor() << "Description" << colors.OldColor() << ": " << Description << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandsDescription(stream, colors);
    stream << Endl;
    PrintParentOptions(stream, config, colors);
    config.Opts->SetCmdLineDescr(stream.Str());
}


}
}
