#include "cmd_drop.h"

#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>


namespace NYdb::NTpch {

TCommandDrop::TCommandDrop()
    : TTpchCommandBase("drop", {"d"}, "Drop YDB database")
{}

void TCommandDrop::Config(TConfig&) {
}

int TCommandDrop::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TTpchRunner tpch{driver, Path};
    NTable::TTableClient tableClient(driver);
    tpch.DropTables(tableClient, true /* removeDir */);
    driver.Stop(true);
    return 0;
}

} // namespace NYdb::NTpch
