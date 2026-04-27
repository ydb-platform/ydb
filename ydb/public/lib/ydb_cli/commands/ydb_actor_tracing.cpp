#include "ydb_actor_tracing.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_actor_tracing.h>
#include <ydb/public/lib/ydb_cli/common/command_utils.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

#include <stdexcept>

namespace NYdb::NConsoleClient {

namespace {

std::vector<uint32_t> ParseNodeIds(const TString& raw) {
    std::vector<uint32_t> ids;
    if (raw.empty()) {
        return ids;
    }
    TVector<TString> parts;
    StringSplitter(raw).SplitBySet(", \t").SkipEmpty().Collect(&parts);
    ids.reserve(parts.size());
    for (auto& part : parts) {
        TString token = TString(StripString(part));
        if (token.empty()) {
            continue;
        }
        uint32_t value = 0;
        if (!TryFromString<uint32_t>(token, value)) {
            ythrow yexception() << "Invalid node id '" << token << "': expected unsigned 32-bit integer";
        }
        ids.push_back(value);
    }
    return ids;
}

void AddNodeIdsOption(TClientCommand::TConfig& config, TString* dst) {
    config.Opts->AddLongOption("node-ids",
            "List of node ids to operate on, separated by commas or whitespace "
            "(e.g. \"1,2,3\" or \"1 2 3\"). If omitted, applies to all nodes currently "
            "visible to the receiving server.")
        .RequiredArgument("IDS")
        .StoreResult(dst);
}

} // anonymous namespace

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
    AddNodeIdsOption(config, &NodeIdsRaw);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingStart::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    NActorTracing::TTraceStartSettings settings;
    for (uint32_t id : ParseNodeIds(NodeIdsRaw)) {
        settings.AppendNodeIds(id);
    }

    auto result = client.TraceStart(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    Cout << "Actor tracing started." << Endl;
    return EXIT_SUCCESS;
}

TCommandActorTracingStop::TCommandActorTracingStop()
    : TYdbCommand("stop", {}, "Stop actor tracing")
{}

void TCommandActorTracingStop::Config(TConfig& config) {
    TYdbCommand::Config(config);
    AddNodeIdsOption(config, &NodeIdsRaw);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingStop::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    NActorTracing::TTraceStopSettings settings;
    for (uint32_t id : ParseNodeIds(NodeIdsRaw)) {
        settings.AppendNodeIds(id);
    }

    auto result = client.TraceStop(settings).GetValueSync();
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
    AddNodeIdsOption(config, &NodeIdsRaw);
    config.SetFreeArgsNum(0);
}

int TCommandActorTracingFetch::Run(TConfig& config) {
    auto driver = std::make_unique<TDriver>(CreateDriver(config));
    NActorTracing::TActorTracingClient client(*driver);

    NActorTracing::TTraceFetchSettings settings;
    for (uint32_t id : ParseNodeIds(NodeIdsRaw)) {
        settings.AppendNodeIds(id);
    }

    auto result = client.TraceFetch(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& data = result.GetTraceData();
    TFileOutput out(OutputPath);
    out.Write(data.data(), data.size());
    out.Flush();

    Cout << "Trace data written to " << OutputPath << " (" << data.size() << " bytes)" << Endl;
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
