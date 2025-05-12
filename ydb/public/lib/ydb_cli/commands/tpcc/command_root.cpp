#include "command_root.h"

#include "command_run.h"

namespace NYdb::NConsoleClient {

TCommandTPCC::TCommandTPCC()
    : TClientCommandTree("tpcc", {}, "YDB TPC-C workload")
{
    AddCommand(std::make_unique<TCommandTPCCRun>());
}

} // namesapce NYdb::NConsoleClient
