#include "ydb_actor_tracing.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_actor_tracing.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <util/stream/file.h>

namespace NYdb::NConsoleClient {

TCommandActorTracing::TCommandActorTracing()
    : TClientCommandTree("actor-tracing", {}, "Actor tracing operations")
{
    AddCommand(std::make_unique<TCommandActorTracingStart>());
    AddCommand(std::make_unique<TCommandActorTracingStop>());
    AddCommand(std::make_unique<TCommandActorTracingFetch>());
}

TCommandActorTracingStart::TCommandActorTracingStart()
    : TYdbCommand("start", {}, "Start actor tracing")
{}

void TCommandActorTracingStart::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingStart::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    auto result = client.TraceStart().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Actor tracing started." << Endl;
    return EXIT_SUCCESS;
}

TCommandActorTracingStop::TCommandActorTracingStop()
    : TYdbCommand("stop", {}, "Stop actor tracing")
{}

void TCommandActorTracingStop::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingStop::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    auto result = client.TraceStop().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Actor tracing stopped." << Endl;
    return EXIT_SUCCESS;
}

TCommandActorTracingFetch::TCommandActorTracingFetch()
    : TYdbCommand("fetch", {}, "Fetch collected actor trace data")
{}

void TCommandActorTracingFetch::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('o', "output", "Output file path for trace data")
        .DefaultValue("actors_trace.bin")
        .StoreResult(&OutputPath);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingFetch::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    auto result = client.TraceFetch().GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& data = result.GetTraceData();
    TFileOutput out(OutputPath);
    out.Write(data.data(), data.size());
    out.Flush();

    Cout << "Trace data written to " << OutputPath << " (" << data.size() << " bytes)" << Endl;
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
