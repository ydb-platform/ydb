#include "ydb_debug.h"

#include "ydb_latency.h"
#include "ydb_ping.h"

namespace NYdb::NConsoleClient {

TCommandDebug::TCommandDebug()
    : TClientCommandTree("debug", {}, "YDB cluster debug operations")
{
    AddCommand(std::make_unique<TCommandLatency>());
    AddCommand(std::make_unique<TCommandPing>());
}

} // NYdb::NConsoleClient
