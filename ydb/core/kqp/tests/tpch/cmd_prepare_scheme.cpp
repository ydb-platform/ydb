#include "cmd_prepare_scheme.h"

#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>

#include <util/string/split.h>

namespace NYdb::NTpch {

TCommandPrepareScheme::TCommandPrepareScheme()
    : TTpchCommandBase("prepare-scheme", {"ps"}, "Prepare YDB database scheme only")
{}

void TCommandPrepareScheme::Config(TConfig& config) {
    config.Opts->AddLongOption('p', "partitions", "Uniform partitions count in comma-separated key=value format.\n"
                                                  "Ex.: -p nation=1,lineitem=10,...")
        .Optional()
        .Handler1T<TStringBuf>([this](TStringBuf opt) {
            for (TStringBuf kv : StringSplitter(opt).Split(',').SkipEmpty()) {
                TStringBuf table, partitions;
                kv.Split('=', table, partitions);
                TablePartitions.emplace(table, FromString<ui32>(partitions));
            }
        });
    config.SetFreeArgsNum(0);
}

int TCommandPrepareScheme::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TTpchRunner tpch{driver, Path};
    tpch.CreateTables(TablePartitions, true);
    driver.Stop(true);
    return 0;
}

} // namespace NYdb::NTpch
