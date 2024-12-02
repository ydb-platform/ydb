#include "ydb_debug.h"

#include "ydb_ping.h"

namespace NYdb::NConsoleClient {

TCommandDebug::TCommandDebug()
    : TClientCommandTree("debug", {}, "YDB cluster debug operations")
{
    AddCommand(std::make_unique<TCommandPing>());
}

} // NYdb::NConsoleClient
