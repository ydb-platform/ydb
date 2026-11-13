#include <ydb/apps/bridge_skipper/config/config.h>
#include <ydb/apps/bridge_skipper/http/http.h>
#include <ydb/apps/bridge_skipper/runtime/events.h>
#include <ydb/apps/bridge_skipper/runtime/skipper.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/json/json_reader.h>
#include <util/generic/guid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/stream/output.h>

#include <chrono>
#include <csignal>
#include <thread>
#include <limits>

namespace NBridgeSkipper {

namespace {

volatile std::sig_atomic_t StopRequested = 0;

void OnSignal(int) {
    StopRequested = 1;
}

NActors::NLog::EPriority LogPriority(const TString& level) {
    using namespace NActors::NLog;
    if (level == "trace") {
        return PRI_TRACE;
    }
    if (level == "debug") {
        return PRI_DEBUG;
    }
    if (level == "warn") {
        return PRI_WARN;
    }
    if (level == "error") {
        return PRI_ERROR;
    }
    if (level == "fatal") {
        return PRI_CRIT;
    }
    return PRI_INFO;
}

int StartupFailed(TStringBuf category, TStringBuf message) {
    NJson::TJsonValue value(NJson::JSON_MAP);
    value["event"] = "startup_failed";
    value["level"] = "error";
    value["category"] = TString(category);
    value["message"] = TString(message.substr(0, 1024));
    Cerr << NJson::WriteJson(value, false, true) << "\n";
    return 1;
}

class TCaptureBackend final : public TLogBackend {
public:
    TCaptureBackend(std::shared_ptr<TDiagnosticStore> store, const TLoadedCredentials& credentials)
        : Store(std::move(store))
        , Redactor(credentials)
        , Stderr(NActors::CreateStderrBackend()) {
    }

    void WriteData(const TLogRecord& record) override {
        // SDK issue strings have already been redacted at decode. Redact again
        // before retaining diagnostics, then tee exactly those bytes to stderr.
        const auto text = Redactor.Redact(TStringBuf(record.Data, record.Len), std::numeric_limits<size_t>::max());
        NJson::TJsonValue json;
        TString level = "info";
        if (NJson::ReadJsonTree(text, &json) && json["priority"].IsString()) {
            level = json["priority"].GetString();
            level.to_lower();
            if (level == "warning") {
                level = "warn";
            }
            if (level == "crit" || level == "alert" || level == "emerg") {
                level = "fatal";
            }
        }
        TInstant eventAt = TInstant::Now();
        if (json["microseconds"].IsUInteger()) {
            eventAt = TInstant::MicroSeconds(json["microseconds"].GetUInteger());
        }
        Store->Log(level, TString(text.substr(0, 4096)), eventAt);
        Stderr->WriteData(TLogRecord(record.Priority, text.data(), text.size(), record.MetaFlags));
    }

    void ReopenLog() override {
        Stderr->ReopenLog();
    }

private:
    std::shared_ptr<TDiagnosticStore> Store;
    TRedactor Redactor;
    TAutoPtr<TLogBackend> Stderr;
};

int Run(
    const TConfig& config,
    const TLoadedCredentials& credentials,
    std::shared_ptr<TStateFile> stateFile)
{
    using namespace NActors;
    auto snapshots = std::make_shared<TSnapshotStore>();
    auto shutdownRequested = std::make_shared<std::atomic<bool>>(false);
    auto admission = std::make_shared<TCommandAdmission>();
    const auto bootId = CreateGuidAsString();
    auto diagnostics = std::make_shared<TDiagnosticStore>(bootId, config.Logging.Level);
    auto seed = std::make_shared<TSkipperSnapshot>();
    seed->ClusterId = config.ClusterId;
    seed->Mode = stateFile->GetState().Mode;
    seed->ControllerBootId = bootId;
    seed->Diagnostics = diagnostics;
    seed->BootUtc = TInstant::Now();
    seed->PersistentState = stateFile->GetState();
    seed->GenerationHighWater = seed->PersistentState.GenerationHighWater;
    seed->Selection.Blocker = EBlockerReason::NoView;
    seed->Decision.Kind = EDecisionKind::Blocked;
    seed->Decision.Blocker = EBlockerReason::NoView;
    seed->Decision.Reason = ToString(EBlockerReason::NoView);
    for (const auto& [pile, endpoints] : config.Endpoints) {
        for (const auto& endpoint : endpoints) {
            TEndpointSnapshot item;
            item.Uri = endpoint.Uri;
            item.Pile = pile;
            seed->Endpoints.push_back(std::move(item));
        }
    }
    snapshots->Publish(std::move(seed));

    const TActorId loggerId(1, "skipper-log");
    auto logging = MakeIntrusive<NLog::TSettings>(loggerId, NActorsServices::LOGGER, LogPriority(config.Logging.Level));
    logging->Append(NActorsServices::EServiceCommon_MIN, NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name);
    logging->Append(SkipperLogComponent, SkipperLogComponent + 1, [](NLog::EComponent) -> const TString& {
        static const TString name = "BRIDGE_SKIPPER";
        return name;
    });
    logging->Format = NLog::TSettings::JSON_FORMAT;
    logging->SetEnableStructuredLogInJson(true);
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 2;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[2]);
    setup->Executors[0] = new TBasicExecutorPool(0, 2, 50, "skipper");
    setup->Executors[1] = new TIOExecutorPool(1, 1, "skipper-controller-io");
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));
    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::HTSwap, 0));
    setup->LocalServices.emplace_back(loggerId, TActorSetupCmd(
        new TLoggerActor(logging, new TCaptureBackend(diagnostics, credentials), MakeIntrusive<NMonitoring::TDynamicCounters>()),
        TMailboxType::HTSwap, 0));
    auto system = MakeHolder<TActorSystem>(setup, nullptr, logging);
    std::shared_ptr<IYdbClients> clients;
    bool started = false;
    TActorId skipper;
    const char* failureCategory = "startup";
    // Returns false when the actor did not acknowledge the stop.
    const auto shutdown = [&] {
        admission->Close();
        shutdownRequested->store(true);
        // Main may wait; no actor handler waits on a future. The actor stops
        // starting calls before the driver drains its callbacks. Processed drain
        // outcomes may be persisted; restart reconciles any remaining pending record.
        bool acknowledged = true;
        if (skipper) {
            try {
                system->Ask<TEvSkipper::TEvStopped>(skipper, MakeHolder<TEvSkipper::TEvStop>(), TDuration::Seconds(5)).GetValueSync();
            } catch (...) {
                // A stalled actor: the pending record is durable, so stop anyway.
                // The raw line does not depend on the executors that may be stalled.
                Cerr << "{\"event\":\"shutdown_failed\",\"level\":\"error\",\"category\":\"stop_ack\"}\n";
                acknowledged = false;
            }
            skipper = {};
        }
        if (clients) {
            clients->Stop();
        }
        if (started) {
            system->Stop();
            system->Cleanup();
        }
        return acknowledged;
    };
    try {
        system->Start();
        started = true;
        // Driver and all callback clocks use the actor system's thread-safe clock.
        // The system object outlives Stop(true) and the client's destruction.
        clients = CreateYdbClients(config, credentials, [actorSystem = system.Get()] {
            return actorSystem->Monotonic();
        });
        // A failed bind must exit before the controller makes any YDB call.
        const auto proxy = system->Register(NHttp::CreateHttpProxy(), TMailboxType::HTSwap, 0);
        const auto http = system->Register(CreateHttpActor(snapshots, proxy, config.Http, admission), TMailboxType::HTSwap, 0);
        system->Ask<TEvSkipper::TEvReadyAck>(http, MakeHolder<TEvSkipper::TEvReady>(), TDuration::Seconds(10)).GetValueSync();
        failureCategory = "listen";
        const auto listenEndpoint = ParseEndpoint("grpc://" + config.Http.Listen);
        auto listen = MakeHolder<NHttp::TEvHttpProxy::TEvAddListeningPort>(listenEndpoint.Port);
        listen->Address = listenEndpoint.Host.StartsWith('[')
            ? TString(TStringBuf(listenEndpoint.Host).substr(1, listenEndpoint.Host.size() - 2)) : listenEndpoint.Host;
        system->Ask<NHttp::TEvHttpProxy::TEvConfirmListen>(proxy, std::move(listen), TDuration::Seconds(10)).GetValueSync();
        failureCategory = "startup";
        TSkipperOptions options;
        options.Config = config;
        options.Clients = clients;
        options.StateFile = std::move(stateFile);
        options.Snapshots = snapshots;
        options.BootId = bootId;
        options.ShutdownRequested = shutdownRequested;
        options.Diagnostics = diagnostics;
        skipper = system->Register(CreateSkipperActor(std::move(options)), TMailboxType::HTSwap, 1);
        system->Ask<TEvSkipper::TEvReadyAck>(skipper, MakeHolder<TEvSkipper::TEvReady>(), TDuration::Seconds(10)).GetValueSync();
        admission->Open(skipper);
        size_t endpointCount = 0;
        for (const auto& [pile, endpoints] : config.Endpoints) {
            (void)pile;
            endpointCount += endpoints.size();
        }
        YDB_LOG_INFO_CTX_COMP(*system, SkipperLogComponent, "Skipper started",
            {"event", "startup"}, {"cluster_id", config.ClusterId},
            {"mode", ToString(snapshots->Read()->Mode)}, {"build", GetProgramCommitId()},
            {"endpoints", endpointCount}, {"http_listen", config.Http.Listen});
        if (credentials.Token && config.Endpoints.begin()->second.front().Scheme == "grpc") {
            YDB_LOG_WARN_CTX_COMP(*system, SkipperLogComponent, "Token is sent over plaintext gRPC",
                {"event", "plaintext_token"});
        }
        failureCategory = "runtime";
        while (!StopRequested) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return shutdown() ? 0 : 1;
    } catch (...) {
        if (started) {
            YDB_LOG_ERROR_CTX_COMP(*system, SkipperLogComponent, "Runtime failure",
                {"event", "runtime_failed"}, {"category", failureCategory});
        }
        shutdown();
        if (started) {
            return 1;
        }
        throw;
    }
}

} // namespace
} // namespace NBridgeSkipper

int main(int argc, char** argv) {
    using namespace NBridgeSkipper;
    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    std::signal(SIGPIPE, SIG_IGN);
    // Argument, config, credential and state errors are built from literals and
    // key paths, never from credential or file bytes, so their text is printed.
    TStringBuf category = "arguments";
    try {
        TVector<TString> raw;
        for (int i = 1; i < argc; ++i) {
            raw.emplace_back(argv[i]);
        }
        const auto arguments = ParseEntrypointArguments(raw);
        if (arguments.Action == EEntrypointAction::Help) {
            Cout << EntrypointHelp();
            return 0;
        }
        if (arguments.Action == EEntrypointAction::Version) {
            Cout << "bridge_skipper build=" << GetProgramCommitId() << " state_schema=3\n";
            return 0;
        }
        category = "config";
        const auto config = LoadConfig(arguments.ConfigFile);
        category = "credentials";
        const auto credentials = LoadCredentials(config);
        if (arguments.Action == EEntrypointAction::CheckConfig) {
            Cout << "configuration valid\n";
            return 0;
        }
        category = "state";
        std::shared_ptr<TStateFile> stateFile = TStateFile::Open(arguments.StateDir, config.ClusterId);
        if (arguments.ClearPendingMutation) {
            const auto error = stateFile->ClearPending();
            if (!error.empty()) {
                return StartupFailed(category, "cannot clear pending mutation");
            }
            NJson::TJsonValue overrideLog(NJson::JSON_MAP);
            overrideLog["event"] = "pending_mutation_override";
            overrideLog["level"] = "warn";
            overrideLog["message"] = "cleared at startup";
            Cerr << NJson::WriteJson(overrideLog, false, true) << "\n";
        }
        category = "runtime";
        return Run(config, credentials, std::move(stateFile));
    } catch (const std::exception& error) {
        if (category == "arguments" || category == "config" || category == "credentials" || category == "state") {
            return StartupFailed(category, error.what());
        }
        // Low-level exceptions and remote payloads never enter startup output.
        return StartupFailed(category, "Check the state directory and listener availability");
    }
}
