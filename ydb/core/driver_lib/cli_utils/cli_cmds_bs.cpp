#include "cli.h"
#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

std::unique_ptr<TClientCommand> CreateClientCommandDisk();
std::unique_ptr<TClientCommand> CreateClientCommandGenConfig();
std::unique_ptr<TClientCommand> CreateClientCommandBsConfig();

TClientCommandBlobStorage::TClientCommandBlobStorage()
    : TClientCommandTree("blobstorage", { "bs" }, "Blob Storage management")
{
    AddCommand(CreateClientCommandDisk());
    AddCommand(CreateClientCommandGenConfig());
    AddCommand(CreateClientCommandBsConfig());
}

}
}
