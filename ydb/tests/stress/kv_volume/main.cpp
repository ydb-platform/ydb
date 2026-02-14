#include "initial_load_display.h"
#include "keyvalue_client.h"
#include "run_display.h"
#include "run_stats.h"
#include "types.h"
#include "utils.h"
#include "worker.h"
#include "worker_load.h"

#include <ydb/tests/stress/kv_volume/protos/config.pb.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <exception>
#include <chrono>
#include <csignal>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>

namespace {

using namespace NKvVolumeStress;

volatile std::sig_atomic_t StopRequestedBySignal = 0;

void HandleStopSignal(int) {
    StopRequestedBySignal = 1;
}

THashMap<TString, TString> BuildPresetConfigs() {
    return {
        {
            "common_channel_read",
            R"pb(
partition_mode: OnePartition
volume_config {
    path: "kv_volume"
    partition_count: 16
    channel_media: ["ssd", "ssd", "ssd"]
}
initial_data {
    write_commands {
        size: 1048576
        count: 5
        channel: 0
    }
}
actions {
    name: "read"
    period_us: 10000
    action_data_mode {
        worker {}
    }
    action_command {
        read {
            size: 1024
            count: 5
            verify_data: true
        }
    }
}
)pb"
        },
        {
            "inline_channel_read",
            R"pb(
partition_mode: OnePartition
volume_config {
    path: "kv_volume"
    partition_count: 16
    channel_media: ["ssd", "ssd", "ssd"]
}
initial_data {
    write_commands {
        size: 1048576
        count: 5
        channel: 1
    }
}
actions {
    name: "read"
    period_us: 10000
    action_data_mode {
        worker {}
    }
    action_command {
        read {
            size: 1024
            count: 5
            verify_data: true
        }
    }
}
)pb"
        },
        {
            "write_read_delete",
            R"pb(
partition_mode: OnePartition
volume_config {
    path: "kv_volume"
    partition_count: 16
    channel_media: ["ssd", "ssd", "ssd"]
}
actions {
    name: "write"
    period_us: 50000
    action_data_mode {
        worker {}
    }
    action_command {
        write {
            size: 4096
            count: 5
            channel: 0
        }
    }
}
actions {
    name: "read"
    parent_action: "write"
    action_data_mode {
        from_prev_actions {
            action_name: "write"
        }
    }
    action_command {
        read {
            size: 1024
            count: 5
            verify_data: true
        }
    }
}
actions {
    name: "delete"
    parent_action: "read"
    action_data_mode {
        from_prev_actions {
            action_name: "write"
        }
    }
    action_command {
        delete {
            count: 5
        }
    }
}
)pb"
        },
    };
}

bool ParseConfigText(const TString& text, NKikimrKeyValue::KeyValueVolumeStressLoad* config, TString* error) {
    if (!google::protobuf::TextFormat::ParseFromString(text, config)) {
        *error = "failed to parse textproto config";
        return false;
    }
    return true;
}

bool LoadConfig(const TOptions& options, NKikimrKeyValue::KeyValueVolumeStressLoad* config, TString* error) {
    if (!options.ConfigPath.empty()) {
        try {
            const TString data = TFileInput(options.ConfigPath).ReadAll();
            return ParseConfigText(data, config, error);
        } catch (const std::exception& e) {
            *error = TStringBuilder() << "failed to read config file: " << e.what();
            return false;
        }
    }

    const auto presets = BuildPresetConfigs();
    const auto it = presets.find(options.ConfigName);
    if (it == presets.end()) {
        *error = TStringBuilder() << "unknown config preset: " << options.ConfigName;
        return false;
    }

    return ParseConfigText(it->second, config, error);
}

bool ValidateConfig(const NKikimrKeyValue::KeyValueVolumeStressLoad& config, TString* error) {
    if (!config.has_volume_config()) {
        *error = "volume_config is required";
        return false;
    }

    if (config.volume_config().path().empty()) {
        *error = "volume_config.path is required";
        return false;
    }

    if (config.volume_config().partition_count() == 0) {
        *error = "volume_config.partition_count must be > 0";
        return false;
    }

    THashSet<TString> actionNames;
    for (const auto& action : config.actions()) {
        if (action.name().empty()) {
            *error = "action.name must be non-empty";
            return false;
        }
        if (!actionNames.insert(action.name()).second) {
            *error = TStringBuilder() << "duplicate action name: " << action.name();
            return false;
        }
    }

    for (const auto& action : config.actions()) {
        if (action.has_parent_action() && !action.parent_action().empty() && !actionNames.contains(action.parent_action())) {
            *error = TStringBuilder() << "unknown parent_action: " << action.parent_action();
            return false;
        }

        if (action.has_action_data_mode()
            && action.action_data_mode().Mode_case() == NKikimrKeyValue::ActionDataMode::kFromPrevActions) {
            for (const auto& sourceAction : action.action_data_mode().from_prev_actions().action_name()) {
                if (!actionNames.contains(sourceAction)) {
                    *error = TStringBuilder() << "unknown source action in from_prev_actions: " << sourceAction;
                    return false;
                }
            }
        }
    }

    return true;
}

std::pair<ui64, ui64> CalculateInitialLoadTotals(
    const NKikimrKeyValue::KeyValueVolumeStressLoad& config,
    ui32 workersCount)
{
    ui64 totalBytesPerWorker = 0;
    ui64 totalCommandsPerWorker = 0;

    for (const auto& writeCommand : config.initial_data().write_commands()) {
        totalBytesPerWorker += static_cast<ui64>(writeCommand.size()) * writeCommand.count();
        ++totalCommandsPerWorker;
    }

    return {
        totalBytesPerWorker * workersCount,
        totalCommandsPerWorker * workersCount,
    };
}

TOptions ParseOptions(int argc, char** argv) {
    TOptions options;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("KeyValue volume stress workload");

    opts.AddLongOption("endpoint", "YDB endpoint, for example grpc://localhost:2135")
        .StoreResult(&options.Endpoint)
        .DefaultValue("grpc://localhost:2135");

    opts.AddLongOption("database", "Database path")
        .Required()
        .StoreResult(&options.Database);

    opts.AddLongOption("duration", "Workload duration in seconds")
        .StoreResult(&options.Duration)
        .DefaultValue("120");

    opts.AddLongOption("in-flight", "Number of worker threads")
        .StoreResult(&options.InFlight)
        .DefaultValue("1");

    opts.AddLongOption("version", "KeyValue API version: v1 or v2")
        .StoreResult(&options.Version)
        .DefaultValue("v1");

    opts.AddLongOption("config", "Path to textproto config")
        .StoreResult(&options.ConfigPath);

    opts.AddLongOption("config-name", "Preset config name")
        .StoreResult(&options.ConfigName);

    opts.AddLongOption("allow-errors", "Return 0 even if runtime operation errors happened")
        .NoArgument()
        .SetFlag(&options.AllowErrors);

    opts.AddLongOption("verbose", "Verbose workload logs")
        .NoArgument()
        .SetFlag(&options.Verbose);

    opts.AddLongOption("no-tui", "Disable TUI even in interactive terminal, use plain text updates")
        .NoArgument()
        .SetFlag(&options.NoTui);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);
    (void)parseResult;

    if (options.InFlight == 0) {
        throw std::runtime_error("--in-flight must be > 0");
    }

    if (options.Duration == 0) {
        throw std::runtime_error("--duration must be > 0");
    }

    if (options.ConfigPath.empty() == options.ConfigName.empty()) {
        throw std::runtime_error("exactly one of --config or --config-name must be provided");
    }

    if (options.Version != "v1" && options.Version != "v2") {
        throw std::runtime_error("--version must be v1 or v2");
    }

    return options;
}

int RunWorkload(const TOptions& options, const NKikimrKeyValue::KeyValueVolumeStressLoad& config) {
    const TString hostPort = ParseHostPort(options.Endpoint);
    const TString volumePath = MakeVolumePath(options.Database, config.volume_config().path());

    TVector<TString> actionNames;
    actionNames.reserve(config.actions_size());
    for (const auto& action : config.actions()) {
        actionNames.push_back(action.name());
    }

    TRunStats stats(std::move(actionNames));
    TWorkerLoadTracker workerLoadTracker(options.InFlight);
    const auto [initialTotalBytes, initialTotalCommands] = CalculateInitialLoadTotals(config, options.InFlight);
    TInitialLoadProgress initialLoadProgress(initialTotalBytes, initialTotalCommands);
    double runElapsedSeconds = 0.0;
    bool interrupted = false;

    {
        std::unique_ptr<IKeyValueClient> setupClient = MakeKeyValueClient(hostPort, options.Version);
        TString error;
        TVector<TString> channels;
        channels.reserve(config.volume_config().channel_media_size());
        for (const auto& media : config.volume_config().channel_media()) {
            channels.push_back(media);
        }

        if (!setupClient->CreateVolume(volumePath, config.volume_config().partition_count(), channels, &error)) {
            Cerr << "CreateVolume failed: " << error << Endl;
            return 2;
        }
    }

    {
        TVector<std::unique_ptr<TWorker>> workers;
        workers.reserve(options.InFlight);

        for (ui32 workerId = 0; workerId < options.InFlight; ++workerId) {
            workers.emplace_back(std::make_unique<TWorker>(
                workerId,
                options,
                config,
                hostPort,
                volumePath,
                stats,
                &initialLoadProgress,
                &workerLoadTracker,
                &StopRequestedBySignal));
        }

        if (initialTotalCommands > 0) {
            TInitialLoadDisplayController initialLoadDisplay(initialLoadProgress, stats, options.NoTui, options.Verbose);
            initialLoadDisplay.Start();
            for (const auto& worker : workers) {
                if (StopRequestedBySignal != 0) {
                    interrupted = true;
                    break;
                }
                worker->LoadInitialData();
            }
            initialLoadDisplay.Stop();
        } else {
            for (const auto& worker : workers) {
                if (StopRequestedBySignal != 0) {
                    interrupted = true;
                    break;
                }
                worker->LoadInitialData();
            }
        }

        if (StopRequestedBySignal != 0) {
            interrupted = true;
        }

        if (!interrupted) {
            const auto runStart = std::chrono::steady_clock::now();
            const auto runEndAt = runStart + std::chrono::seconds(options.Duration);
            TRunDisplayController display(stats, &workerLoadTracker, options.Duration, options.NoTui, options.Verbose);
            display.Start();

            TVector<std::thread> threads;
            threads.reserve(options.InFlight);

            for (const auto& worker : workers) {
                threads.emplace_back([workerPtr = worker.get(), runEndAt] {
                    workerPtr->Run(runEndAt);
                });
            }

            for (auto& thread : threads) {
                thread.join();
            }

            display.Stop();

            if (StopRequestedBySignal != 0) {
                interrupted = true;
            }

            runElapsedSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                std::chrono::steady_clock::now() - runStart
            ).count();
        }
    }

    {
        std::unique_ptr<IKeyValueClient> setupClient = MakeKeyValueClient(hostPort, options.Version);
        TString error;
        if (!setupClient->DropVolume(volumePath, &error)) {
            Cerr << "DropVolume failed: " << error << Endl;
            return 2;
        }
    }

    stats.PrintSummary(runElapsedSeconds);

    if (interrupted) {
        Cerr << "Interrupted by signal, workload stopped and volume dropped" << Endl;
        return 130;
    }

    if (stats.GetTotalErrors() > 0 && !options.AllowErrors) {
        return 1;
    }

    return 0;
}

} // namespace

int main(int argc, char** argv) {
    try {
        StopRequestedBySignal = 0;
        std::signal(SIGINT, HandleStopSignal);
        std::signal(SIGTERM, HandleStopSignal);

        const TOptions options = ParseOptions(argc, argv);

        NKikimrKeyValue::KeyValueVolumeStressLoad config;
        TString error;

        if (!LoadConfig(options, &config, &error)) {
            Cerr << "Config loading failed: " << error << Endl;
            return 2;
        }

        if (!ValidateConfig(config, &error)) {
            Cerr << "Config validation failed: " << error << Endl;
            return 2;
        }

        return RunWorkload(options, config);
    } catch (const std::exception& e) {
        Cerr << "Fatal error: " << e.what() << Endl;
        return 2;
    }
}
