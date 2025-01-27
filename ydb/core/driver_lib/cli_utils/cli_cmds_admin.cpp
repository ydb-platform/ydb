#include "cli.h"
#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

TClientCommandAdmin::TClientCommandAdmin()
    : TClientCommandTree("admin", {}, "YDB management and administration")
{
    AddCommand(std::make_unique<TClientCommandTablet>());
    AddCommand(std::make_unique<TClientCommandNode>());
    AddCommand(std::make_unique<TClientCommandDebug>());
    AddCommand(std::make_unique<TClientCommandBlobStorage>());
    AddCommand(std::make_unique<TClientCommandTenant>());
    AddCommand(std::make_unique<TClientCommandConsole>());
}

}
}
