#include "cmd_run_query.h"

#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>


namespace NYdb::NTpch {

TCommandRunQuery::TCommandRunQuery()
    : TTpchCommandBase("run-query", {"q"}, "Run single TPC-H query")
{}

void TCommandRunQuery::Config(TConfig& config){
    config.Opts->AddLongOption('p', "profile", "Execute query with enabled profiling mode and save profile to this file")
        .StoreResult(&Profile);
    config.SetFreeArgsNum(1);
    config.Opts->SetFreeArgDefaultTitle("Query number (1-22)");
}

int TCommandRunQuery::Run(TConfig& config){
    auto driver = CreateDriver(config);
    TTpchRunner tpch{driver, Path};

    auto queryN = FromString<ui32>(config.ParseResult->GetFreeArgs()[0]);
    auto profile = !Profile.empty();

    TInstant timeBefore = TInstant::Now();
    auto it = tpch.RunQuery(queryN, profile);

    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            Y_ABORT_UNLESS(streamPart.EOS(), "endpoint %s\n%s", streamPart.GetEndpoint().c_str(),
                streamPart.GetIssues().ToString().c_str());
            break;
        }

        if (streamPart.HasResultSet()) {
            const auto& result = streamPart.GetResultSet();
            const auto& columns = result.GetColumnsMeta();

            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                for (size_t i = 0; i < columns.size(); ++i) {
                    Cout << columns[i].Name << "=" << FormatValueYson(parser.GetValue(i)) << ", ";
                }
                Cout << Endl;
            }
        }

        if (streamPart.HasQueryStats() && Profile) {
            const auto& profile = streamPart.HasQueryStats();
            // TODO: support multiple profile files
            TFileOutput out{Profile};
            out.Write(profile);
            out.Flush();
        }
    }

    TDuration time = TInstant::Now() - timeBefore;
    Cout << "Total time: " << time << Endl;

    driver.Stop(true);
    return 0;
}

} // namespace NYdb::NTpch
