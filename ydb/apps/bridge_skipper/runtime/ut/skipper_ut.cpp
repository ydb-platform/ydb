#include <ydb/apps/bridge_skipper/runtime/skipper.h>
#include <ydb/apps/bridge_skipper/runtime/events.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

#include <sys/stat.h>
#include <unistd.h>

namespace NBridgeSkipper {
namespace {

const TString A = "grpc://a:1";
const TString A2 = "grpc://a:2";
const TString A3 = "grpc://a:3";
const TString B = "grpc://b:1";
const TString C = "grpc://c:1";
const TString FastRounds = "{round_interval_seconds: 1, confirm_seconds: 1, bsc_warmup_seconds: 20, read_timeout_seconds: 5, selfcheck_operation_timeout_seconds: 12, evidence_ttl_seconds: 30}";
const TString SlowRounds = "{round_interval_seconds: 10, confirm_seconds: 10, bsc_warmup_seconds: 20, read_timeout_seconds: 5, selfcheck_operation_timeout_seconds: 12, evidence_ttl_seconds: 30}";

const TString FastRoundsWith30SecondWarmup = "{round_interval_seconds: 1, confirm_seconds: 1, bsc_warmup_seconds: 30, read_timeout_seconds: 5, selfcheck_operation_timeout_seconds: 12, evidence_ttl_seconds: 30}";

struct TStateCall {
    TRpcRequest Request;
    EYdbMethod Method;
    TMonotonic SentAt;
    std::function<void(TStateResult)> Completed;
    bool Done = false;
};

struct THealthCall {
    TRpcRequest Request;
    TMonotonic SentAt;
    std::function<void(TSelfCheckResult)> Completed;
    bool Done = false;
};

struct TUpdateCall {
    TRpcRequest Request;
    TPlan Plan;
    TMonotonic SentAt;
    std::function<void(TUpdateResult)> Completed;
    bool Done = false;
};

class TFakeClients final : public IYdbClients {
public:
    std::function<TMonotonic()> Now;
    std::vector<TStateCall> States;
    std::vector<THealthCall> Health;
    std::vector<TUpdateCall> Updates;
    std::function<void()> PrepareUpdate;

    void GetState(TRpcRequest request, EYdbMethod method, std::function<void(TStateResult)> completed) override {
        for (const auto& call : States) {
            UNIT_ASSERT(call.Done || call.Request.Endpoint != request.Endpoint || call.Method != method);
        }
        States.push_back({std::move(request), method, Now(), std::move(completed)});
    }

    void SelfCheck(TRpcRequest request, std::function<void(TSelfCheckResult)> completed) override {
        for (const auto& call : Health) {
            UNIT_ASSERT(call.Done || call.Request.Endpoint != request.Endpoint);
        }
        Health.push_back({std::move(request), Now(), std::move(completed)});
    }

    bool Update(TRpcRequest request, TPlan plan, TUpdateAuthorization authorization,
        std::function<void(TUpdateResult)> completed) override
    {
        if (PrepareUpdate) {
            PrepareUpdate();
        }
        if (Now() >= authorization.ValidUntil) {
            return false;
        }
        for (const auto& call : Updates) {
            UNIT_ASSERT(call.Done || call.Request.Endpoint != request.Endpoint);
        }
        Updates.push_back({std::move(request), std::move(plan), Now(), std::move(completed)});
        return true;
    }

    std::vector<TDiscoveredEndpoint> Inventory;
    std::vector<TString> Removed;
    std::set<TString> FailedInventory;
    std::vector<TString> InventoryCalls;

    void Discover(TRpcRequest request, std::function<void(TInventoryResult)> completed) override {
        TInventoryResult result;
        InventoryCalls.push_back(request.Endpoint);
        const bool failed = FailedInventory.contains(request.Endpoint);
        result.Rpc.Request = std::move(request);
        result.Rpc.Method = EYdbMethod::Inventory;
        result.Rpc.SentAt = result.Rpc.FinishedAt = Now();
        result.Rpc.Success = !failed;
        result.Rpc.Status = failed ? "UNSUPPORTED" : "SUCCESS";
        if (!failed) {
            result.Endpoints = Inventory;
        }
        completed(std::move(result));
    }

    void AddEndpoint(const TEndpoint&, const TString&) override {}
    void RemoveEndpoint(const TString& uri) override { Removed.push_back(uri); }

    void Stop() override {
    }
};

NJson::TJsonValue Json(TStringBuf text) {
    NJson::TJsonValue result;
    NJson::ReadJsonTree(text, &result, true);
    return result;
}

class TRecordLogBackend final : public TLogBackend {
public:
    explicit TRecordLogBackend(std::vector<TString>& records)
        : Records(records) {
    }

    void WriteData(const TLogRecord& record) override {
        Records.emplace_back(record.Data, record.Len);
    }

    void ReopenLog() override {
    }

private:
    std::vector<TString>& Records;
};

TLoadedCredentials TestCredentials() {
    TLoadedCredentials credentials;
    credentials.Token = "test-private-token";
    return credentials;
}

struct THarness {
    // The runtime and its logger are destroyed before their captured records.
    std::vector<TString> Records;
    NActors::TTestActorRuntimeBase Runtime;
    TTempDir Directory;
    std::shared_ptr<TStateFile> File;
    std::shared_ptr<TFakeClients> Clients = std::make_shared<TFakeClients>();
    std::shared_ptr<TSnapshotStore> Store = std::make_shared<TSnapshotStore>();
    // Emulates the production SDK boundary, which redacts before delivery.
    const TRedactor Redactor{TestCredentials()};
    TConfig Config;
    NActors::TActorId Actor;
    NActors::TActorId Edge;
    ui64 Generation = 7;
    TNamedPileStates Piles = {{"a", EPileState::Primary}, {"b", EPileState::Synchronized}};
    std::set<std::pair<TString, EYdbMethod>> Held;
    std::map<TString, TString> Identities;
    std::map<TString, ui32> NodeIds;
    // SelfCheck replies from these endpoints omit location.pile.
    std::set<TString> PileLess;
    std::set<TString> Failed = {"b"};
    std::map<TString, std::set<TString>> EndpointFailures;
    std::set<std::pair<TString, EYdbMethod>> FailedMethods;
    // The SDK status of the calls listed in FailedMethods.
    NYdb::EStatus FailedStatus = NYdb::EStatus::TIMEOUT;
    // Connectivity reports show these piles without node quorum.
    std::set<TString> NoQuorum;
    std::map<TString, std::set<TString>> EndpointNoQuorum;
    std::set<TString> FallbackEndpoints;
    // Decisive static-group failures, distinct from incomplete fallback parts.
    std::set<TString> FallbackFailed;
    std::function<void(const TPersistentState&)> AfterWrite;
    std::function<void()> AfterFailedWrite;
    bool Fallback = false;
    bool Truncated = false;
    // Endpoints of pile b report b as PROMOTED.
    bool Conflict = false;
    bool OmitConnectivity = false;
    bool Binding = false;
    std::optional<ui64> ConnectivityGeneration;
    // SelfCheck replies arrive this long after they were sent.
    std::optional<TDuration> SelfCheckDelay;
    std::map<TString, TDuration> StateDelays;

    std::shared_ptr<std::atomic<bool>> Shutdown = std::make_shared<std::atomic<bool>>(false);

    explicit THarness(
        EMode mode = EMode::Auto,
        std::optional<TPersistentState> restored = {},
        size_t survivorEndpoints = 1,
        const TString& timing = FastRounds,
        size_t healthEndpointsPerPile = 2)
    {
        Runtime.SetLogBackend(new TRecordLogBackend(Records));
        Runtime.Initialize();
        Runtime.UpdateCurrentTime(TInstant::Seconds(100), true);
        auto settings = Runtime.GetLogSettings(0);
        settings->Append(SkipperLogComponent, SkipperLogComponent + 1, [](NActors::NLog::EComponent) -> const TString& {
            static const TString name = "BRIDGE_SKIPPER";
            return name;
        });
        settings->Format = NActors::NLog::TSettings::JSON_FORMAT;
        settings->SetEnableStructuredLogInJson(true);
        Runtime.SetLogPriority(SkipperLogComponent, NActors::NLog::PRI_DEBUG);
        File = TStateFile::Open(Directory.Name(), "test");
        if (restored) {
            UNIT_ASSERT_VALUES_EQUAL(File->Write(*restored), "");
        }
        Config = ParseConfig(TString("cluster_id: test\nendpoints: {a: [grpc://a:1], b: [grpc://b:1]}\ntiming: ") + timing + "\n");
        Config.HealthEndpointsPerPile = healthEndpointsPerPile;
        auto state = File->GetState();
        state.Mode = mode;
        UNIT_ASSERT_VALUES_EQUAL(File->Write(state), "");
        for (const auto& extra : {A2, A3}) {
            if (Config.Endpoints["a"].size() < survivorEndpoints) {
                Config.Endpoints["a"].push_back(ParseEndpoint(extra));
            }
        }
        Clients->Now = [&] {
            return Runtime.GetCurrentMonotonicTime();
        };
        Edge = Runtime.AllocateEdgeActor();
        TSkipperOptions options;
        options.Config = Config;
        options.Clients = Clients;
        options.StateFile = File;
        options.Snapshots = Store;
        options.WriteState = [&](const TPersistentState& state) {
            const auto error = File->Write(state);
            if (error.empty() && AfterWrite) {
                AfterWrite(state);
            }
            if (!error.empty() && AfterFailedWrite) {
                AfterFailedWrite();
            }
            return error;
        };
        options.ScheduleTicks = false;
        options.ShutdownRequested = Shutdown;
        Actor = Runtime.Register(CreateSkipperActor(std::move(options)));
        Tick();
    }

    TCommandResult Command(TControllerCommand command) {
        Runtime.SendAsync(new NActors::IEventHandle(Actor, Edge, new TEvSkipper::TEvCommand(std::move(command)), 0, 917));
        auto reply = Runtime.GrabEdgeEvent<TEvSkipper::TEvCommandResult>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 917);
        return reply->Get()->Result;
    }

    TCommandResult Mode(EMode mode, TString id = "mode-1") {
        const auto snapshot = Store->Read();
        TControllerCommand command;
        command.Kind = ECommandKind::Mode;
        command.Mode = mode;
        command.RequestId = id;
        command.ExpectedBootId = snapshot->ControllerBootId;
        command.ExpectedModeRevision = snapshot->ModeRevision;
        command.SnapshotRevision = snapshot->SnapshotRevision;
        return Command(command);
    }

    TCommandResult Review(EOperationKind action = EOperationKind::Failover, std::optional<TString> target = {}) {
        TControllerCommand command;
        command.Action = action;
        command.Target = target;
        command.SnapshotRevision = Store->Read()->SnapshotRevision;
        return Command(command);
    }

    TCommandResult Execute(const TString& plan, const TString& id) {
        TControllerCommand command;
        command.Kind = ECommandKind::Execute;
        command.PlanId = plan;
        command.RequestId = id;
        return Command(command);
    }

    void Tick() {
        Runtime.SendAsync(new NActors::IEventHandle(Actor, Edge, new TEvSkipper::TEvTick()));
        Runtime.GrabEdgeEvent<TEvSkipper::TEvTickComplete>(Edge);
    }

    // A directory in place of the temporary file fails every state write.
    void BreakWrites() {
        UNIT_ASSERT_VALUES_EQUAL(mkdir((Directory.Name() + "/state.json.tmp").c_str(), 0700), 0);
    }

    void RestoreWrites() {
        UNIT_ASSERT_VALUES_EQUAL(rmdir((Directory.Name() + "/state.json.tmp").c_str()), 0);
    }

    TRpcResult Rpc(const TRpcRequest& request, EYdbMethod method, TMonotonic sentAt) {
        TRpcResult rpc;
        rpc.Request = request;
        rpc.Method = method;
        rpc.SentAt = sentAt;
        rpc.FinishedAt = Runtime.GetCurrentMonotonicTime();
        rpc.Success = true;
        rpc.SdkStatus = NYdb::EStatus::SUCCESS;
        rpc.Status = "SUCCESS";
        return rpc;
    }

    TStateResult StateReply(const TStateCall& call) {
        TStateResult result;
        result.Rpc = Rpc(call.Request, call.Method, call.SentAt);
        TClusterView view;
        view.Generation = Generation;
        view.Piles = Piles;
        if (Conflict && call.Request.Pile == "b") {
            view.Piles["b"] = EPileState::Promoted;
        }
        view.Endpoint = call.Request.Endpoint;
        view.SentAt = call.SentAt;
        result.View = view;
        if (call.Method == EYdbMethod::ConnectivityState && !OmitConnectivity) {
            TConnectivity connectivity;
            connectivity.Generation = ConnectivityGeneration.value_or(Generation);
            connectivity.BindingInProgress = Binding;
            connectivity.VantagePile = call.Request.Pile;
            connectivity.SentAt = call.SentAt;
            for (const auto& [name, state] : Piles) {
                (void)state;
                const auto& noQuorum = EndpointNoQuorum.contains(call.Request.Endpoint)
                    ? EndpointNoQuorum.at(call.Request.Endpoint) : NoQuorum;
                connectivity.Piles[name] = noQuorum.contains(name)
                    ? TPileConnectivity{3, 0, false, true, false} : TPileConnectivity{3, 3, true, true, true};
            }
            result.Connectivity = std::move(connectivity);
        }
        return result;
    }

    void ReplyReads() {
        for (auto& call : Clients->States) {
            if (call.Done || Held.contains({call.Request.Endpoint, call.Method})) {
                continue;
            }
            const auto delay = StateDelays.find(call.Request.Endpoint);
            if (delay != StateDelays.end() && call.SentAt + delay->second > Runtime.GetCurrentMonotonicTime()) {
                continue;
            }
            call.Done = true;
            auto result = StateReply(call);
            if (FailedMethods.contains({call.Request.Endpoint, call.Method})) {
                result.Rpc.Success = false;
                result.Rpc.SdkStatus = FailedStatus;
                result.Rpc.Status = SdkStatusName(FailedStatus);
                result.View.reset();
                result.Error = result.Rpc.Status;
                result.Connectivity.reset();
            }
            call.Completed(std::move(result));
        }
        for (auto& call : Clients->Health) {
            if (call.Done || Held.contains({call.Request.Endpoint, EYdbMethod::SelfCheck})) {
                continue;
            }
            if (SelfCheckDelay && call.SentAt + *SelfCheckDelay > Runtime.GetCurrentMonotonicTime()) {
                continue;
            }
            call.Done = true;
            TSelfCheckResult result;
            result.Rpc = Rpc(call.Request, EYdbMethod::SelfCheck, call.SentAt);
            if (FailedMethods.contains({call.Request.Endpoint, EYdbMethod::SelfCheck})) {
                result.Rpc.Success = false;
                result.Rpc.SdkStatus = FailedStatus;
                result.Rpc.Status = SdkStatusName(FailedStatus);
                call.Completed(std::move(result));
                continue;
            }
            Ydb::Monitoring::SelfCheckResult proto;
            const auto& failures = EndpointFailures.contains(call.Request.Endpoint) ? EndpointFailures.at(call.Request.Endpoint) : Failed;
            for (const auto& failed : failures) {
                auto* issue = proto.add_issue_log();
                issue->set_type("BRIDGE_GROUP");
                issue->set_status(Ydb::Monitoring::StatusFlag::RED);
                issue->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(failed);
            }
            if (Fallback || FallbackEndpoints.contains(call.Request.Endpoint)) {
                auto* issue = proto.add_issue_log();
                issue->set_type("STORAGE");
                issue->set_status(Ydb::Monitoring::StatusFlag::RED);
                issue->set_message("System tablet BSC didn't provide information");
            }
            for (const auto& failed : FallbackFailed) {
                auto* group = proto.add_issue_log();
                group->set_id("group-" + failed);
                group->set_type("STORAGE_GROUP");
                group->set_status(Ydb::Monitoring::StatusFlag::RED);
                group->mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_pile()->set_name(failed);
                group->add_reason("disk-" + failed);
                auto* disk = proto.add_issue_log();
                disk->set_id("disk-" + failed);
                disk->set_type("VDISK");
                disk->set_status(Ydb::Monitoring::StatusFlag::RED);
                disk->set_message("VDisk is not available");
            }
            if (Truncated) {
                auto* issue = proto.add_issue_log();
                issue->set_type("HEALTHCHECK_STATUS");
                issue->set_status(Ydb::Monitoring::StatusFlag::RED);
            }
            TSelfCheckReply reply;
            reply.Endpoint = call.Request.Endpoint;
            reply.SentAt = call.SentAt;
            if (!PileLess.contains(call.Request.Endpoint)) {
                reply.ResponderPile = Identities.contains(call.Request.Endpoint) ? Identities.at(call.Request.Endpoint) : call.Request.Pile;
            }
            reply.Classification = ClassifySelfCheck(proto);
            result.Reply = std::move(reply);
            result.ResponderNodeId = NodeIds.contains(call.Request.Endpoint) ? NodeIds.at(call.Request.Endpoint)
                : call.Request.Pile == "a" ? 1 : call.Request.Pile == "b" ? 2 : 3;
            call.Completed(std::move(result));
        }
    }

    void Step() {
        Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        Tick();
        ReplyReads();
        Tick();
    }

    void Run(size_t seconds) {
        for (size_t i = 0; i < seconds; ++i) {
            Step();
        }
    }

    void Outcome(size_t index, NYdb::EStatus status) {
        auto& call = Clients->Updates.at(index);
        UNIT_ASSERT(!call.Done);
        call.Done = true;
        TUpdateResult result;
        result.Rpc = Rpc(call.Request, EYdbMethod::Update, call.SentAt);
        result.Rpc.SdkStatus = status;
        result.Rpc.Status = SdkStatusName(status);
        result.Rpc.Success = status == NYdb::EStatus::SUCCESS;
        result.Rpc.Issues = Redactor.Redact("credential test-private-token; remote detail");
        result.Plan = call.Plan;
        result.Outcome = ClassifyUpdateOutcome(status);
        call.Completed(std::move(result));
        Tick();
    }

    size_t CallsAt(EYdbMethod method, TMonotonic sentAt) const {
        size_t count = 0;
        if (method == EYdbMethod::SelfCheck) {
            for (const auto& call : Clients->Health) {
                count += call.SentAt == sentAt;
            }
        } else {
            for (const auto& call : Clients->States) {
                count += call.Method == method && call.SentAt == sentAt;
            }
        }
        return count;
    }

    TEndpointSnapshot Endpoint(const TString& uri) const {
        const auto snapshot = Store->Read();
        for (const auto& endpoint : snapshot->Endpoints) {
            if (endpoint.Uri == uri) {
                return endpoint;
            }
        }
        UNIT_FAIL("missing endpoint");
        return {};
    }

    std::vector<NJson::TJsonValue> Logs(TStringBuf eventName) {
        // A logger mailbox barrier ensures all preceding actor logs reached the
        // actual native JSON backend before inspecting its output.
        Runtime.SendAsync(new NActors::IEventHandle(Runtime.GetLogSettings(0)->LoggerActorId, Edge,
            new NActors::TLogComponentLevelRequest(NActors::NLog::PRI_DEBUG, SkipperLogComponent)));
        Runtime.GrabEdgeEvent<NActors::TLogComponentLevelResponse>(Edge);
        std::vector<NJson::TJsonValue> result;
        for (const auto& line : Records) {
            auto record = Json(line);
            if (record["event"].IsString() && record["event"].GetString() == eventName) {
                result.push_back(std::move(record));
            }
        }
        return result;
    }
};

TPersistentState Pending(const TExactUpdate& update = {{"b", EPileState::Disconnected}}) {
    return {"test", 7, TPendingMutation{7, update}};
}

void VerifyPileLessReplyRetainsCandidateVeto(bool fallback) {
    THarness fixture;
    for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
        fixture.Held.insert({A, method});
    }
    fixture.NoQuorum = {"a"};
    fixture.Fallback = fallback;
    fixture.Run(5);
    UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Confirmed);
    UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoCandidate);
    UNIT_ASSERT(fixture.Clients->Updates.empty());
    const auto cachedSend = fixture.Endpoint(B).SelfCheck->SentAt;
    fixture.PileLess.insert(B);
    fixture.Run(3);
    // The only health source is now pile-less. It must not erase either the
    // BSC availability/RED verdict or a still-fresh fallback part on b.
    UNIT_ASSERT(fixture.Clients->Updates.empty());
    UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoCandidate);
    UNIT_ASSERT(fixture.Endpoint(B).Identity == EEndpointIdentity::Verified);
    UNIT_ASSERT(fixture.Endpoint(B).LastError.Contains("ENDPOINT_PILE_UNVERIFIED"));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(B).SelfCheck->SentAt, cachedSend);
    UNIT_ASSERT(fixture.Endpoint(B).SelfCheck->ResponderPile == TString("b"));
    // Keeping the observation never refreshes its send time. Once it expires,
    // node quorum alone cannot qualify b without its own usable reply.
    fixture.Run(31);
    UNIT_ASSERT(fixture.Clients->Updates.empty());
    UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoCandidate);
    fixture.PileLess.erase(B);
    fixture.Fallback = true;
    fixture.Failed.clear();
    fixture.Run(3);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
    UNIT_ASSERT(fixture.Clients->Updates.front().Plan.Update == TExactUpdate({
        {"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
}

TControllerCommand ModeCommand(const THarness& fixture, EMode mode) {
    const auto snapshot = fixture.Store->Read();
    TControllerCommand command;
    command.Kind = ECommandKind::Mode;
    command.Mode = mode;
    command.RequestId = "transient-mode";
    command.ExpectedBootId = snapshot->ControllerBootId;
    command.ExpectedModeRevision = snapshot->ModeRevision;
    command.SnapshotRevision = snapshot->SnapshotRevision;
    return command;
}

void VerifyManualDispatchExpiry(EOperationKind action, bool slowPersistence) {
    THarness fixture(EMode::Manual);
    if (action != EOperationKind::Failover) {
        fixture.Failed.clear();
    }
    fixture.Run(22);
    const auto review = action == EOperationKind::Failover
        ? fixture.Review() : fixture.Review(action, TString("b"));
    UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
    bool prepared = false;
    const auto expire = [&] {
        prepared = true;
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(31));
    };
    if (slowPersistence) {
        fixture.AfterWrite = [&](const TPersistentState& state) {
            if (!prepared && state.PendingMutation) {
                expire();
            }
        };
    } else {
        fixture.Clients->PrepareUpdate = expire;
    }
    const auto refused = fixture.Execute(review.Body["plan_id"].GetString(), "expired-dispatch");
    UNIT_ASSERT(prepared);
    UNIT_ASSERT(fixture.Clients->Updates.empty());
    const auto snapshot = fixture.Store->Read();
    UNIT_ASSERT(snapshot->StateWriteError.empty());
    UNIT_ASSERT(snapshot->PersistentState.PendingMutation);
    UNIT_ASSERT(snapshot->PersistentState.PendingMutation->Kind == action);
    UNIT_ASSERT(!snapshot->LastMutation);
    UNIT_ASSERT(snapshot->Counters.MutationTotal.empty());
    UNIT_ASSERT_VALUES_EQUAL(snapshot->PendingAttempts, 0);
    UNIT_ASSERT(!snapshot->PendingFirstSendUtc);
    UNIT_ASSERT(!snapshot->PendingLastSendUtc);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Decision.Reason, "EVIDENCE_EXPIRED");
    UNIT_ASSERT(fixture.Logs("mutation_sent").empty());
    UNIT_ASSERT(fixture.Logs("mutation_outcome").empty());
    UNIT_ASSERT_VALUES_EQUAL(refused.Status, 409);
    UNIT_ASSERT_VALUES_EQUAL(refused.Body["error"].GetString(), "EVIDENCE_EXPIRED");
}

void VerifyDispatchAtTtlBoundary(EMode mode, bool slowPersistence) {
    THarness fixture(EMode::Manual);
    fixture.Run(22);
    // Keep one admitted SelfCheck older than every view and connectivity
    // report, so its send time determines the launch authorization.
    fixture.Held.insert({A, EYdbMethod::SelfCheck});
    fixture.Step();
    const auto evidenceSentAt = fixture.Endpoint(A).SelfCheck->SentAt;
    const auto validUntil = evidenceSentAt + fixture.Config.Timing.EvidenceTtl;
    UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
    UNIT_ASSERT(fixture.Store->Read()->Selection.ValidUntil > validUntil);
    UNIT_ASSERT(fixture.Endpoint(B).SelfCheck->SentAt > evidenceSentAt);
    for (const auto& [pile, snapshot] : fixture.Store->Read()->Piles) {
        (void)pile;
        for (const auto& report : snapshot.Connectivity) {
            UNIT_ASSERT(report.SentAt > evidenceSentAt);
        }
    }
    TString reviewedPlan;
    if (mode == EMode::Manual) {
        const auto review = fixture.Review();
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        reviewedPlan = review.Body["plan_id"].GetString();
    }
    bool prepared = false;
    const auto expire = [&] {
        UNIT_ASSERT(!prepared);
        prepared = true;
        const auto now = fixture.Runtime.GetCurrentMonotonicTime();
        UNIT_ASSERT(now < validUntil);
        fixture.Runtime.AdvanceCurrentTime(validUntil - now);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GetCurrentMonotonicTime(), validUntil);
    };
    if (slowPersistence) {
        fixture.AfterWrite = [&](const TPersistentState& state) {
            if (!prepared && state.PendingMutation) {
                expire();
            }
        };
    } else {
        fixture.Clients->PrepareUpdate = expire;
    }
    if (mode == EMode::Manual) {
        const auto refused = fixture.Execute(reviewedPlan, "ttl-boundary");
        UNIT_ASSERT_VALUES_EQUAL(refused.Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(refused.Body["error"].GetString(), "EVIDENCE_EXPIRED");
    } else {
        UNIT_ASSERT_VALUES_EQUAL(fixture.Mode(EMode::Auto, "ttl-boundary").Status, 200);
    }
    UNIT_ASSERT(prepared);
    UNIT_ASSERT(fixture.Clients->Updates.empty());
    const auto snapshot = fixture.Store->Read();
    const auto pending = fixture.File->GetState().PendingMutation;
    UNIT_ASSERT(pending);
    UNIT_ASSERT(snapshot->PersistentState.PendingMutation == pending);
    UNIT_ASSERT(snapshot->StateWriteError.empty());
    UNIT_ASSERT(!snapshot->LastMutation);
    UNIT_ASSERT(snapshot->Counters.MutationTotal.empty());
    UNIT_ASSERT_VALUES_EQUAL(snapshot->PendingAttempts, 0);
    UNIT_ASSERT(!snapshot->PendingFirstSendUtc);
    UNIT_ASSERT(!snapshot->PendingLastSendUtc);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Decision.Reason, "EVIDENCE_EXPIRED");
    UNIT_ASSERT(fixture.Logs("mutation_sent").empty());
    UNIT_ASSERT(fixture.Logs("mutation_outcome").empty());
    // Fresh observations authorize the retained overlay and prove that local
    // expiry did not leave an outstanding update slot behind.
    fixture.AfterWrite = {};
    fixture.Clients->PrepareUpdate = {};
    fixture.Held.clear();
    fixture.Run(3);
    if (mode == EMode::Manual) {
        const auto review = fixture.Review();
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Execute(review.Body["plan_id"].GetString(), "fresh-evidence").Status, 202);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
    UNIT_ASSERT(fixture.Clients->Updates.front().Plan.Update == pending->Update);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.front().Plan.ExpectedGeneration, pending->ExpectedGeneration);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Logs("mutation_sent").size(), 1);
}

} // namespace

Y_UNIT_TEST_SUITE(TSkipperActor) {
    Y_UNIT_TEST(DiscoveryKeepsInventoryAccessAfterAllSeedsAreRemoved) {
        THarness h;
        h.Failed.clear();
        h.Run(35);
        h.Piles["c"] = EPileState::Synchronized;
        const TString healthOnly = "grpc://c:2";
        const TString rejectedProbe = "grpc://c:3";
        const TString inventory = "grpc://c:4";
        const TString addedLater = "grpc://d:1";
        h.NodeIds[healthOnly] = 4;
        h.NodeIds[rejectedProbe] = 6;
        h.NodeIds[inventory] = 3; // full services on a different port of the first health node
        h.NodeIds[addedLater] = 5;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}, {ParseEndpoint(healthOnly), "c", 4},
            {ParseEndpoint(rejectedProbe), "c", 6}, {ParseEndpoint(inventory), "c", 3}};
        h.Clients->FailedInventory = {C, healthOnly, rejectedProbe};
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Endpoint(healthOnly).Identity == EEndpointIdentity::Verified);
        h.Piles = {{"c", EPileState::Primary}};
        ++h.Generation;
        h.Clients->FailedInventory.insert(A);
        h.Clients->FailedInventory.insert(B);
        h.Run(40);
        UNIT_ASSERT(h.Endpoint(inventory).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(std::ranges::find(h.Clients->InventoryCalls, rejectedProbe) != h.Clients->InventoryCalls.end());
        UNIT_ASSERT(std::ranges::find(h.Clients->InventoryCalls, inventory) != h.Clients->InventoryCalls.end());
        size_t retained = 0;
        for (const auto& endpoint : h.Store->Read()->Endpoints) {
            retained += endpoint.Pile == "c";
        }
        UNIT_ASSERT_VALUES_EQUAL(retained, h.Config.HealthEndpointsPerPile + 1);
        // Only the retained full-service address can discover another pile now.
        h.Piles["d"] = EPileState::Synchronized;
        ++h.Generation;
        h.Clients->Inventory.push_back({ParseEndpoint(addedLater), "d", 5});
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(addedLater).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DiscoveryRecoversFromCachedAddressesAfterEveryViewExpires) {
        THarness h(EMode::Auto, {}, 1, "{}", 1);
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        const TString replacement = "grpc://c-replacement:1";
        const TString addedLater = "grpc://d:1";
        h.NodeIds[replacement] = 4;
        h.NodeIds[addedLater] = 5;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        // The original seed piles are removed while Skipper keeps running.
        h.Piles = {{"c", EPileState::Primary}};
        ++h.Generation;
        h.Clients->FailedInventory = {A, B};
        h.Run(35);
        h.Clients->Inventory.push_back({ParseEndpoint(replacement), "c", 4});
        h.Run(6);
        for (const auto& call : h.Clients->States) {
            UNIT_ASSERT_VALUES_UNEQUAL(call.Request.Endpoint, replacement);
        }
        const auto highWater = h.File->GetState().GenerationHighWater;
        // The selected port still serves Config/Discovery, but both state
        // variants and Monitoring stop answering. Its retention grace outlasts
        // the evidence TTL, so the next cached probe must work without a view.
        for (const auto& uri : {A, B, C}) {
            for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
                h.FailedMethods.insert({uri, method});
            }
        }
        ++h.Generation;
        h.Held.insert({replacement, EYdbMethod::SelfCheck});
        h.Run(12);
        h.Clients->FailedInventory.insert(C);
        UNIT_ASSERT(!h.Store->Read()->Selection.View);
        UNIT_ASSERT(h.Store->Read()->Decision.Blocker == EBlockerReason::NoView);
        UNIT_ASSERT_VALUES_EQUAL(h.File->GetState().GenerationHighWater, highWater);
        UNIT_ASSERT(h.Clients->Updates.empty());
        // A cached advertisement and even a fresh Bridge reply are only
        // bootstrap data until the new locator verifies its advertised identity.
        UNIT_ASSERT(h.Endpoint(replacement).View);
        UNIT_ASSERT(h.Endpoint(replacement).Identity == EEndpointIdentity::Unknown);
        h.Held.clear();
        h.Run(3);
        UNIT_ASSERT(h.Store->Read()->Selection.View);
        UNIT_ASSERT_VALUES_EQUAL(h.Store->Read()->Selection.View->Generation, h.Generation);
        UNIT_ASSERT(h.Endpoint(replacement).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Store->Read()->Selection.CurrentEndpoints.contains(replacement));
        // The recovered locator must also resume discovery of future piles.
        h.Piles["d"] = EPileState::Synchronized;
        ++h.Generation;
        h.Clients->Inventory.push_back({ParseEndpoint(addedLater), "d", 5});
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(addedLater).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DiscoveryPendingAliasReservesItsPhysicalNode) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        const TString alias = "grpc://192.0.2.3:1";
        const TString neighbor = "grpc://c-neighbor:1";
        h.NodeIds[alias] = 3;
        h.NodeIds[neighbor] = 4;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Held.insert({C, EYdbMethod::SelfCheck});
        h.Run(2);
        h.Clients->Inventory.push_back({ParseEndpoint(alias), "c", 3});
        h.Clients->Inventory.push_back({ParseEndpoint(neighbor), "c", 4});
        h.Run(10); // a second inventory arrives before the first SelfCheck's deadline
        UNIT_ASSERT(h.Endpoint(neighbor).Identity == EEndpointIdentity::Verified);
        for (const auto& endpoint : h.Store->Read()->Endpoints) {
            UNIT_ASSERT_VALUES_UNEQUAL(endpoint.Uri, alias);
        }
        h.Held.clear();
        h.Run(3);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        for (const auto& uri : {C, alias}) {
            for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
                h.FailedMethods.insert({uri, method});
            }
        }
        h.Run(40);
        h.Piles["b"] = EPileState::Inactive;
        ++h.Generation;
        h.Failed.insert("a");
        h.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.front().Plan.Endpoint, neighbor);
    }

    Y_UNIT_TEST(DiscoveryRetentionCoversTheFullHealthRotation) {
        THarness h(EMode::Auto, {}, 2,
            "{round_interval_seconds: 10, confirm_seconds: 1, bsc_warmup_seconds: 20, "
            "read_timeout_seconds: 1, selfcheck_operation_timeout_seconds: 4, evidence_ttl_seconds: 17}", 1);
        h.Failed.clear();
        h.NodeIds[A2] = 3;
        const TString learned = "grpc://a-learned:1";
        h.NodeIds[learned] = 4;
        h.Clients->Inventory = {{ParseEndpoint(learned), "a", 4}};
        bool sawStaleHealth = false;
        for (size_t i = 0; i < 120; ++i) {
            h.Step();
            for (const auto& endpoint : h.Store->Read()->Endpoints) {
                if (endpoint.Uri == learned && endpoint.SelfCheck
                    && h.Runtime.GetCurrentMonotonicTime() - endpoint.SelfCheck->SentAt >= h.Config.Timing.EvidenceTtl)
                {
                    sawStaleHealth = true;
                }
            }
        }
        UNIT_ASSERT(sawStaleHealth);
        UNIT_ASSERT(h.Endpoint(learned).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Removed.empty());
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DiscoveryRotatesPastPortsWithoutMonitoring) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        const TString second = "grpc://c:2";
        const TString reachable = "grpc://c:3";
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}, {ParseEndpoint(second), "c", 4},
            {ParseEndpoint(reachable), "c", 5}};
        h.NodeIds[reachable] = 5;
        // Both state variants keep answering on these Bridge-only ports.
        h.FailedMethods.insert({C, EYdbMethod::SelfCheck});
        h.FailedMethods.insert({second, EYdbMethod::SelfCheck});
        h.Run(50);
        UNIT_ASSERT(h.Endpoint(reachable).Identity == EEndpointIdentity::Verified);
        h.Piles["b"] = EPileState::Inactive;
        ++h.Generation;
        h.Failed.insert("a");
        h.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.front().Plan.Endpoint, reachable);
    }

    Y_UNIT_TEST(DiscoveryWaitsForSlowInitialVerification) {
        THarness h(EMode::Auto, {}, 1,
            "{round_interval_seconds: 1, confirm_seconds: 1, bsc_warmup_seconds: 20, "
            "read_timeout_seconds: 7, selfcheck_operation_timeout_seconds: 4, evidence_ttl_seconds: 9}");
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.StateDelays[C] = TDuration::Seconds(6);
        h.SelfCheckDelay = TDuration::Seconds(5);
        bool staleViewRetained = false;
        for (size_t i = 0; i < 40; ++i) {
            h.Step();
            const auto snapshot = h.Store->Read();
            for (const auto& endpoint : snapshot->Endpoints) {
                if (endpoint.Uri == C && endpoint.View
                    && h.Runtime.GetCurrentMonotonicTime() - endpoint.View->SentAt >= h.Config.Timing.EvidenceTtl)
                {
                    staleViewRetained = true;
                    // Keeping the client during renewal grants no stale routing authority.
                    UNIT_ASSERT(!snapshot->Selection.CurrentEndpoints.contains(C));
                }
            }
        }
        UNIT_ASSERT(staleViewRetained);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Removed.empty());
        UNIT_ASSERT(h.Clients->Updates.empty());
        h.FailedMethods.insert({C, EYdbMethod::SelfCheck});
        h.Run(30);
        UNIT_ASSERT(!h.Clients->Removed.empty());
    }

    Y_UNIT_TEST(DiscoveryReplacesAnAddressWhoseSelfCheckExpired) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        h.FailedMethods.insert({C, EYdbMethod::SelfCheck});
        const TString replacement = "grpc://c:2";
        h.Clients->Inventory.push_back({ParseEndpoint(replacement), "c", 3});
        h.Run(50);
        UNIT_ASSERT(h.Endpoint(replacement).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(!h.Clients->Removed.empty());
    }

    Y_UNIT_TEST(DiscoveryReverifiesChangedNodeAtTheSameLearnedAddress) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        h.NodeIds[C] = 4;
        h.Run(2);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Mismatch);
        h.Held.insert({C, EYdbMethod::SelfCheck});
        h.Run(1);
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 4}};
        h.Run(6);
        UNIT_ASSERT(h.Clients->Removed.empty()); // the old invocation still owns its slot
        h.Held.clear();
        h.Run(10);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Removed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Removed.front(), C);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DiscoveryRecoversANewAddressForAnExpiredSeedNode) {
        THarness h;
        h.Failed.clear();
        h.Run(35);
        const TString replacement = "grpc://replacement-b:1";
        h.Clients->Inventory = {{ParseEndpoint(replacement), "b", 2}};
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            h.FailedMethods.insert({B, method});
        }
        h.Run(40);
        UNIT_ASSERT(h.Endpoint(replacement).Identity == EEndpointIdentity::Verified);
        h.Failed.insert("a");
        h.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.front().Plan.Endpoint, replacement);
    }

    Y_UNIT_TEST(DiscoveryRotatesPastUnreachableCandidatesInUnchangedOrder) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        const TString second = "grpc://c:2";
        const TString reachable = "grpc://c:3";
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}, {ParseEndpoint(second), "c", 4},
            {ParseEndpoint(reachable), "c", 5}};
        h.NodeIds[reachable] = 5;
        for (const auto& uri : {C, second}) {
            for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
                h.FailedMethods.insert({uri, method});
            }
        }
        h.Run(40);
        UNIT_ASSERT(h.Endpoint(reachable).Identity == EEndpointIdentity::Verified);
        size_t count = 0;
        for (const auto& endpoint : h.Store->Read()->Endpoints) {
            count += endpoint.Pile == "c";
        }
        UNIT_ASSERT(count <= h.Config.HealthEndpointsPerPile);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DiscoveryBoundsNewAddressesAndDeduplicatesVerifiedNodeAliases) {
        THarness h;
        h.Failed.clear();
        h.Run(35);
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint("grpc://alias-a:1"), "a", 1},
            {ParseEndpoint(C), "c", 3}, {ParseEndpoint("grpc://c:2"), "c", 4},
            {ParseEndpoint("grpc://c:3"), "c", 5}};
        h.NodeIds["grpc://c:2"] = 4;
        h.NodeIds["grpc://c:3"] = 5;
        h.Run(35);
        size_t learned = 0;
        for (const auto& endpoint : h.Store->Read()->Endpoints) {
            learned += endpoint.Pile == "c";
            UNIT_ASSERT_VALUES_UNEQUAL(endpoint.Uri, "grpc://alias-a:1");
        }
        UNIT_ASSERT_VALUES_EQUAL(learned, h.Config.HealthEndpointsPerPile);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(RemovedSeedsCanBeReusedByANewPileAtTheSameAddress) {
        THarness h;
        h.Failed.clear();
        h.Run(35);
        h.Piles["b"] = EPileState::Inactive;
        ++h.Generation;
        h.Run(3);
        h.Piles.erase("b");
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(B), "c", 3}};
        h.Run(35);
        const auto endpoint = h.Endpoint(B);
        UNIT_ASSERT_VALUES_EQUAL(endpoint.Pile, "c");
        UNIT_ASSERT(endpoint.Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(NewPileIsDiscoveredAndCanReceiveGuardedFailover) {
        THarness h;
        h.Failed.clear();
        h.Run(35);
        h.Piles["c"] = EPileState::Synchronized;
        ++h.Generation;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Updates.empty());
        // The original secondary is removed; the new pile is the only survivor.
        h.Piles["b"] = EPileState::Inactive;
        ++h.Generation;
        h.Run(2);
        h.Piles.erase("b");
        h.Run(2);
        h.Held.insert({A, EYdbMethod::PlainState});
        h.Held.insert({A, EYdbMethod::ConnectivityState});
        h.Held.insert({A, EYdbMethod::SelfCheck});
        h.NoQuorum.insert("a");
        h.Fallback = true;
        h.FallbackFailed.insert("a");
        h.Run(35);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Updates.size(), 1);
        const auto& plan = h.Clients->Updates.front().Plan;
        UNIT_ASSERT_VALUES_EQUAL(plan.Endpoint, C);
        UNIT_ASSERT_VALUES_EQUAL(plan.ExpectedGeneration, h.Generation);
        UNIT_ASSERT((plan.Update == TExactUpdate{{"a", EPileState::Disconnected}, {"c", EPileState::Primary}}));
    }

    Y_UNIT_TEST(DiscoveryOmissionDoesNotRemoveFreshEvidence) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        h.Clients->Inventory.clear();
        h.Run(40);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(h.Clients->Removed.empty());
        UNIT_ASSERT(h.Clients->Updates.empty());
        UNIT_ASSERT(h.Store->Read()->Decision.Kind == EDecisionKind::Stable);
    }

    Y_UNIT_TEST(UnverifiedDiscoveredViewCannotAdvanceHistory) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.NodeIds[C] = 99;
        h.Run(35);
        UNIT_ASSERT(h.Endpoint(C).Identity == EEndpointIdentity::Mismatch);
        h.Generation = 1000;
        for (const auto& uri : {A, B}) {
            h.Held.insert({uri, EYdbMethod::PlainState});
            h.Held.insert({uri, EYdbMethod::ConnectivityState});
        }
        h.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(h.File->GetState().GenerationHighWater, 7);
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(RemovedLearnedEndpointDrainsLateRepliesBeforeClientDeletion) {
        THarness h;
        h.Failed.clear();
        h.Piles["c"] = EPileState::Synchronized;
        h.Clients->Inventory = {{ParseEndpoint(C), "c", 3}};
        h.Run(35);
        h.Held.insert({C, EYdbMethod::SelfCheck});
        h.Run(1);
        h.Piles["c"] = EPileState::Inactive;
        ++h.Generation;
        h.Run(2);
        UNIT_ASSERT(h.Clients->Removed.empty());
        h.Piles.erase("c");
        h.Held.clear();
        h.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Removed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(h.Clients->Removed.front(), C);
        for (const auto& endpoint : h.Store->Read()->Endpoints) {
            UNIT_ASSERT_VALUES_UNEQUAL(endpoint.Uri, C);
        }
        UNIT_ASSERT(h.Clients->Updates.empty());
    }

    Y_UNIT_TEST(RemovedPileEndpointsAreRetiredWithoutRestart) {
        THarness fixture(EMode::Auto);
        fixture.Failed.clear();
        fixture.Run(35);
        fixture.Piles["b"] = EPileState::Inactive;
        ++fixture.Generation;
        fixture.Run(2);
        for (const auto& endpoint : fixture.Store->Read()->Endpoints) {
            UNIT_ASSERT(endpoint.Pile != "b");
        }
        fixture.Piles.erase("b");
        fixture.Run(2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Piles.size(), 1);
        UNIT_ASSERT(fixture.Store->Read()->Mode == EMode::Auto);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
    }

    Y_UNIT_TEST(DefaultTimingConfirmsLostPrimaryWithin20Seconds) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Healthy);
        const auto faultAt = fixture.Runtime.GetCurrentMonotonicTime();
        fixture.NoQuorum = {"a"};
        fixture.Fallback = true;
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({A, method});
        }
        fixture.Run(20);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.SentAt - faultAt <= TDuration::Seconds(20));
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 7);
        UNIT_ASSERT_VALUES_EQUAL(update.Request.Endpoint, B);
    }

    Y_UNIT_TEST(DefaultTimingConfirmsBscFailureWithinAFewSeconds) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Healthy);
        const auto faultAt = fixture.Runtime.GetCurrentMonotonicTime();
        fixture.Failed = {"a"};
        fixture.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.SentAt - faultAt <= TDuration::Seconds(5));
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 7);
        UNIT_ASSERT_VALUES_EQUAL(update.Request.Endpoint, B);
    }

    Y_UNIT_TEST(DefaultTimingConfirmsFallbackFailureDuringBindingWithin20Seconds) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        const auto faultAt = fixture.Runtime.GetCurrentMonotonicTime();
        fixture.FallbackFailed = {"a"};
        fixture.NoQuorum = {"a"};
        fixture.Binding = true;
        fixture.Fallback = true;
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({A, method});
        }
        fixture.Run(20);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.SentAt - faultAt <= TDuration::Seconds(20));
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 7);
        UNIT_ASSERT_VALUES_EQUAL(update.Request.Endpoint, B);
    }

    Y_UNIT_TEST(CachedSettledLossDoesNotDelayFreshFallbackFailure) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        const auto faultAt = fixture.Runtime.GetCurrentMonotonicTime();
        fixture.Fallback = true;
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({A, method});
        }
        // The survivor first detects loss after interconnect convergence. A
        // settled report is then retained while newer reports are binding.
        fixture.Run(12);
        fixture.NoQuorum = {"a"};
        fixture.FallbackFailed = {"a"};
        fixture.Step();
        fixture.Binding = true;
        fixture.Run(7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.SentAt - faultAt <= TDuration::Seconds(20));
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 7);
    }

    Y_UNIT_TEST(DefaultTimingDoesNotTurnMissingFailureEvidenceIntoAuthorization) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        fixture.NoQuorum = {"a"};
        fixture.Binding = true;
        fixture.Fallback = true;
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({A, method});
        }
        fixture.Run(30);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Unknown);
    }

    Y_UNIT_TEST(DefaultTimingDoesNotFailOverDuringABriefPrimaryOutage) {
        THarness fixture(EMode::Auto, {}, 1, "{}");
        fixture.Failed.clear();
        fixture.Run(35);
        fixture.NoQuorum = {"a"};
        fixture.Fallback = true;
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({A, method});
        }
        fixture.Run(3);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        fixture.Held.clear();
        fixture.NoQuorum.clear();
        fixture.Fallback = false;
        fixture.Run(15);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
    }

    Y_UNIT_TEST(PileLessSelfCheckPreservesFreshBscVeto) {
        VerifyPileLessReplyRetainsCandidateVeto(false);
    }

    Y_UNIT_TEST(PileLessSelfCheckPreservesFreshFallbackVeto) {
        VerifyPileLessReplyRetainsCandidateVeto(true);
    }

    Y_UNIT_TEST(SuccessfulSlowPersistenceCannotDispatchExpiredEvidence) {
        for (const bool highWaterWrite : {false, true}) {
            THarness fixture;
            fixture.EndpointNoQuorum[A] = {"a"};
            fixture.Run(5);
            UNIT_ASSERT(fixture.Clients->Updates.empty());
            UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);
            bool delayed = false;
            fixture.AfterWrite = [&](const TPersistentState& state) {
                if (!delayed && (highWaterWrite ? state.GenerationHighWater == 8 : state.PendingMutation.has_value())) {
                    delayed = true;
                    fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(30));
                }
            };
            if (highWaterWrite) {
                fixture.Generation = 8;
            }
            fixture.EndpointNoQuorum.clear();
            for (size_t i = 0; i < 3 && !delayed; ++i) {
                fixture.Step();
            }
            UNIT_ASSERT(delayed);
            UNIT_ASSERT(fixture.Clients->Updates.empty());
            UNIT_ASSERT(fixture.File->GetState().PendingMutation);
            UNIT_ASSERT(!fixture.Store->Read()->LastMutation);
            UNIT_ASSERT(fixture.Logs("mutation_sent").empty());
            fixture.AfterWrite = {};
            fixture.Run(3);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
            UNIT_ASSERT(fixture.Clients->Updates.front().Plan.Update
                == fixture.File->GetState().PendingMutation->Update);
        }
    }

    Y_UNIT_TEST(PreparationWithinEvidenceLifetimeDispatchesOnce) {
        THarness fixture;
        fixture.EndpointNoQuorum[A] = {"a"};
        fixture.Run(5);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        bool prepared = false;
        fixture.AfterWrite = [&](const TPersistentState& state) {
            if (!prepared && state.PendingMutation) {
                prepared = true;
                fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            }
        };
        fixture.EndpointNoQuorum.clear();
        fixture.Step();
        UNIT_ASSERT(prepared);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Logs("mutation_sent").size(), 1);
    }

    Y_UNIT_TEST(ClientPreparationExpiryDoesNotStrandSlotsOrPublishAnInvocation) {
        THarness fixture;
        fixture.EndpointNoQuorum[A] = {"a"};
        fixture.Run(5);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        bool prepared = false;
        fixture.Clients->PrepareUpdate = [&] {
            prepared = true;
            fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(30));
        };
        fixture.EndpointNoQuorum.clear();
        fixture.Step();
        UNIT_ASSERT(prepared);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        UNIT_ASSERT(fixture.File->GetState().PendingMutation);
        UNIT_ASSERT(!fixture.Store->Read()->LastMutation);
        UNIT_ASSERT(fixture.Store->Read()->Counters.MutationTotal.empty());
        UNIT_ASSERT(fixture.Logs("mutation_sent").empty());
        fixture.Clients->PrepareUpdate = {};
        fixture.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Logs("mutation_sent").size(), 1);
    }

    Y_UNIT_TEST(ManualFailoverPersistenceExpiryReturnsEvidenceExpiredWithoutInvocation) {
        VerifyManualDispatchExpiry(EOperationKind::Failover, true);
    }

    Y_UNIT_TEST(ManualLifecyclePersistenceExpiryReturnsEvidenceExpiredWithoutInvocation) {
        VerifyManualDispatchExpiry(EOperationKind::TakeOffline, true);
    }

    Y_UNIT_TEST(ManualFailoverClientPreparationExpiryReturnsEvidenceExpiredWithoutInvocation) {
        VerifyManualDispatchExpiry(EOperationKind::Failover, false);
    }

    Y_UNIT_TEST(ManualLifecycleClientPreparationExpiryReturnsEvidenceExpiredWithoutInvocation) {
        VerifyManualDispatchExpiry(EOperationKind::TakeOffline, false);
    }

    Y_UNIT_TEST(AutoPersistenceAtEvidenceTtlBoundaryDoesNotDispatch) {
        VerifyDispatchAtTtlBoundary(EMode::Auto, true);
    }

    Y_UNIT_TEST(AutoClientPreparationAtEvidenceTtlBoundaryDoesNotDispatch) {
        VerifyDispatchAtTtlBoundary(EMode::Auto, false);
    }

    Y_UNIT_TEST(ManualPersistenceAtEvidenceTtlBoundaryDoesNotDispatch) {
        VerifyDispatchAtTtlBoundary(EMode::Manual, true);
    }

    Y_UNIT_TEST(ManualClientPreparationAtEvidenceTtlBoundaryDoesNotDispatch) {
        VerifyDispatchAtTtlBoundary(EMode::Manual, false);
    }

    Y_UNIT_TEST(MutationRoutingRequiresTheReceivingNodesOwnComponent) {
        for (const TString& isolated : {A, A2}) {
            THarness fixture(EMode::Auto, {}, 2);
            fixture.EndpointNoQuorum[isolated] = {"a"};
            fixture.FallbackEndpoints.insert(isolated);
            fixture.Run(22);
            const TString capable = isolated == A ? A2 : A;
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.front().Plan.Endpoint, capable);
            fixture.Outcome(0, NYdb::EStatus::TIMEOUT);
            fixture.Run(30);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.back().Plan.Endpoint, capable);
        }
    }

    Y_UNIT_TEST(ObserveManualReviewAndReceiptDoNotDoubleSend) {
        THarness f(EMode::Observe);
        f.Run(22);
        UNIT_ASSERT(f.Clients->Updates.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Review().Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Manual).Status, 200);
        const auto review = f.Review();
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        const auto plan = review.Body["plan_id"].GetString();
        const auto accepted = f.Execute(plan, "execute-1");
        UNIT_ASSERT_VALUES_EQUAL(accepted.Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(plan, "execute-1").Body["operation_id"].GetString(), accepted.Body["operation_id"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(plan, "execute-2").Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Observe, "off").Status, 200);
        UNIT_ASSERT(f.Store->Read()->PersistentState.PendingMutation);
    }

    Y_UNIT_TEST(ReviewAlternatesExcludeTheEffectivePrimaryAfterTheAction) {
        THarness f(EMode::Manual);
        f.Failed.clear();
        f.Run(22);
        auto review = f.Review(EOperationKind::Switchover, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_piles"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_pile_names"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_pile_names"][0].GetString(), "a");
        review = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_piles"].GetUInteger(), 0);
        UNIT_ASSERT(review.Body["alternate_synchronized_pile_names"].GetArray().empty());
        f.Piles["c"] = EPileState::Promoted;
        ++f.Generation;
        f.Run(22);
        review = f.Review(EOperationKind::Switchover, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["after"]["c"].GetString(), "SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_piles"].GetUInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_pile_names"][0].GetString(), "a");
        UNIT_ASSERT_VALUES_EQUAL(review.Body["alternate_synchronized_pile_names"][1].GetString(), "c");
        UNIT_ASSERT(f.Clients->Updates.empty());
    }

    Y_UNIT_TEST(ExecuteExpiresEvidenceWithoutAnInterveningTickAndConsumesPlan) {
        THarness f(EMode::Manual);
        f.Run(22);
        const auto review = f.Review();
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        f.Runtime.AdvanceCurrentTime(TDuration::Seconds(31));
        const auto plan = review.Body["plan_id"].GetString();
        const auto refused = f.Execute(plan, "expired");
        UNIT_ASSERT_VALUES_EQUAL(refused.Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(refused.Body["error"].GetString(), "PLAN_CHANGED");
        UNIT_ASSERT(f.Clients->Updates.empty());
        UNIT_ASSERT(!f.File->GetState().PendingMutation);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(plan, "another").Status, 409);
    }

    Y_UNIT_TEST(ModeRevisionIsConsumedOnRefusalAndOffSaveFailureStopsDispatch) {
        THarness f(EMode::Observe);
        const auto before = f.Store->Read();
        TControllerCommand command;
        command.Kind = ECommandKind::Mode;
        command.Mode = EMode::Auto;
        command.RequestId = "auto";
        command.ExpectedBootId = before->ControllerBootId;
        command.ExpectedModeRevision = before->ModeRevision;
        command.SnapshotRevision = "stale";
        UNIT_ASSERT_VALUES_EQUAL(f.Command(command).Status, 409);
        UNIT_ASSERT(f.Store->Read()->ModeRevision > before->ModeRevision);
        UNIT_ASSERT_VALUES_EQUAL(f.Command(command).Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Manual, "manual").Status, 200);
        f.BreakWrites();
        UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Observe, "off").Status, 503);
        UNIT_ASSERT(f.Store->Read()->Mode == EMode::Observe);
        UNIT_ASSERT(!f.Store->Read()->StateWriteError.empty());
        f.Run(22);
        UNIT_ASSERT(f.Clients->Updates.empty());
        f.RestoreWrites();
        f.Tick();
        UNIT_ASSERT(f.File->GetState().Mode == EMode::Observe);
    }

    Y_UNIT_TEST(ModeRetryAcknowledgesRequestedDurableMode) {
        for (const auto wanted : {EMode::Observe, EMode::Manual, EMode::Auto}) {
            THarness fixture(wanted == EMode::Observe ? EMode::Manual : EMode::Observe);
            fixture.Run(22);
            UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
            const auto command = ModeCommand(fixture, wanted);
            bool failed = false;
            fixture.File->SetDirectorySyncFailureForTests([&] {
                if (!failed) {
                    failed = true;
                    return true;
                }
                return false;
            });
            const auto result = fixture.Command(command);
            UNIT_ASSERT(failed);
            UNIT_ASSERT(fixture.File->GetState().Mode == wanted);
            UNIT_ASSERT(fixture.Store->Read()->Mode == wanted);
            UNIT_ASSERT(fixture.Store->Read()->StateWriteError.empty());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->ModeRevision, command.ExpectedModeRevision + 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, 200);
            UNIT_ASSERT_VALUES_EQUAL(result.Body["mode"].GetString(), ToString(wanted));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), wanted == EMode::Auto ? 1 : 0);
            const auto replay = fixture.Command(command);
            UNIT_ASSERT_VALUES_EQUAL(replay.Status, result.Status);
            UNIT_ASSERT(replay.Body == result.Body);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->ModeRevision, command.ExpectedModeRevision + 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), wanted == EMode::Auto ? 1 : 0);
        }
    }

    Y_UNIT_TEST(ObserveRetryAcknowledgesModeAfterPreRenameFailure) {
        THarness fixture(EMode::Manual);
        fixture.Run(22);
        const auto command = ModeCommand(fixture, EMode::Observe);
        fixture.BreakWrites();
        bool restored = false;
        fixture.AfterFailedWrite = [&] {
            if (!restored) {
                restored = true;
                fixture.RestoreWrites();
            }
        };
        const auto result = fixture.Command(command);
        UNIT_ASSERT(restored);
        UNIT_ASSERT(fixture.File->GetState().Mode == EMode::Observe);
        UNIT_ASSERT(fixture.Store->Read()->Mode == EMode::Observe);
        UNIT_ASSERT(fixture.Store->Read()->StateWriteError.empty());
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(result.Body["mode"].GetString(), "observe");
        const auto replay = fixture.Command(command);
        UNIT_ASSERT_VALUES_EQUAL(replay.Status, result.Status);
        UNIT_ASSERT(replay.Body == result.Body);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->ModeRevision, command.ExpectedModeRevision + 1);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
    }

    Y_UNIT_TEST(FailedActivationRetryDoesNotAcknowledgeRestoredMode) {
        for (const auto wanted : {EMode::Manual, EMode::Auto}) {
            THarness fixture(EMode::Observe);
            fixture.Run(22);
            const auto command = ModeCommand(fixture, wanted);
            fixture.BreakWrites();
            bool restored = false;
            fixture.AfterFailedWrite = [&] {
                if (!restored) {
                    restored = true;
                    fixture.RestoreWrites();
                }
            };
            const auto result = fixture.Command(command);
            UNIT_ASSERT(restored);
            UNIT_ASSERT(fixture.File->GetState().Mode == EMode::Observe);
            UNIT_ASSERT(fixture.Store->Read()->Mode == EMode::Observe);
            UNIT_ASSERT(fixture.Store->Read()->StateWriteError.empty());
            UNIT_ASSERT(fixture.Clients->Updates.empty());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->ModeRevision, command.ExpectedModeRevision + 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, 503);
            UNIT_ASSERT_VALUES_EQUAL(result.Body["error"].GetString(), "STATE_WRITE_FAILED");
            const auto replay = fixture.Command(command);
            UNIT_ASSERT_VALUES_EQUAL(replay.Status, result.Status);
            UNIT_ASSERT(replay.Body == result.Body);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->ModeRevision, command.ExpectedModeRevision + 1);
            UNIT_ASSERT(fixture.Clients->Updates.empty());
        }
    }

    Y_UNIT_TEST(FailedModeSavePublishesTheWriteBlockerWithoutAnotherTick) {
        for (const bool renamed : {false, true}) {
            THarness f(EMode::Manual);
            f.Run(22);
            const auto before = f.Store->Read();
            UNIT_ASSERT(before->Decision.Kind == EDecisionKind::Ready);
            if (renamed) {
                f.File->SetDirectorySyncFailureForTests([] {
                    return true;
                });
            } else {
                f.BreakWrites();
            }
            UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Observe, "off-save-failed").Status, 503);
            const auto failed = f.Store->Read();
            UNIT_ASSERT(failed->Mode == EMode::Observe);
            UNIT_ASSERT(!failed->StateWriteError.empty());
            UNIT_ASSERT(failed->Decision.Kind == EDecisionKind::Blocked);
            UNIT_ASSERT(failed->Decision.Blocker == EBlockerReason::StateWriteFailed);
            UNIT_ASSERT(failed->SnapshotRevision != before->SnapshotRevision);
            UNIT_ASSERT(f.Clients->Updates.empty());
            if (renamed) {
                f.File->SetDirectorySyncFailureForTests({});
            } else {
                f.RestoreWrites();
            }
            f.Tick();
            UNIT_ASSERT(f.Store->Read()->Decision.Kind == EDecisionKind::Ready);
            UNIT_ASSERT(f.File->GetState().Mode == EMode::Observe);
            UNIT_ASSERT(f.Clients->Updates.empty());
        }
    }

    Y_UNIT_TEST(FailedActivationCannotUndoAnUnsavedObserveMode) {
        for (const auto activation : {EMode::Manual, EMode::Auto}) {
            THarness f(EMode::Auto);
            f.Failed.clear();
            f.Run(22);
            f.BreakWrites();
            UNIT_ASSERT_VALUES_EQUAL(f.Mode(EMode::Observe, "off").Status, 503);
            UNIT_ASSERT(f.Store->Read()->Mode == EMode::Observe);
            f.Tick(); // Publish the write-failed decision before reviewing activation.
            UNIT_ASSERT_VALUES_EQUAL(f.Mode(activation, "failed-activation").Status, 503);
            UNIT_ASSERT(f.Store->Read()->Mode == EMode::Observe);
            f.Failed.insert("b");
            f.Run(3);
            UNIT_ASSERT(f.Clients->Updates.empty());
            f.RestoreWrites();
            f.Run(3);
            UNIT_ASSERT(f.File->GetState().Mode == EMode::Observe);
            UNIT_ASSERT(f.Store->Read()->Mode == EMode::Observe);
            UNIT_ASSERT(f.Clients->Updates.empty());
        }
    }

    Y_UNIT_TEST(PostRenameFailedActivationRetainsTheVisibleRequestedMode) {
        for (const auto activation : {EMode::Manual, EMode::Auto}) {
            THarness f(EMode::Observe);
            f.Failed.clear();
            f.Run(22);
            f.File->SetDirectorySyncFailureForTests([] { return true; });
            UNIT_ASSERT_VALUES_EQUAL(f.Mode(activation, "activation").Status, 503);
            UNIT_ASSERT(f.Store->Read()->Mode == activation);
            UNIT_ASSERT(f.File->GetState().Mode == activation);
            f.Failed.insert("b");
            f.Run(3);
            UNIT_ASSERT(f.Clients->Updates.empty());
            f.File->SetDirectorySyncFailureForTests({});
            f.Tick();
            UNIT_ASSERT(f.File->GetState().Mode == activation);
            UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), activation == EMode::Auto ? 1 : 0);
        }
    }

    Y_UNIT_TEST(PostRenameFailedSupersetCannotBeClearedByOlderSuccess) {
        THarness f(EMode::Auto, {}, 2);
        f.Piles["c"] = EPileState::Synchronized;
        f.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        f.File->SetDirectorySyncFailureForTests([] { return true; });
        f.Failed.insert("c");
        f.Run(5);
        const TExactUpdate superset{{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}};
        UNIT_ASSERT(f.File->GetState().PendingMutation->Update == superset);
        f.Outcome(0, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT(f.File->GetState().PendingMutation);
        UNIT_ASSERT(f.File->GetState().PendingMutation->Update == superset);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        f.File->SetDirectorySyncFailureForTests({});
        f.Tick();
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 2);
        UNIT_ASSERT(f.Clients->Updates[1].Plan.Update == superset);
    }

    Y_UNIT_TEST(RejectedMaintenanceRetainsPendingAndBlocksIncompatibleFailover) {
        for (const bool ambiguousFirst : {false, true}) {
            THarness f(EMode::Manual, {}, 2);
            f.Failed.clear();
            f.Run(22);
            auto review = f.Review(EOperationKind::TakeOffline, TString("b"));
            UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
            UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "maintenance").Status, 202);
            if (ambiguousFirst) {
                f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
                review = f.Review(EOperationKind::TakeOffline, TString("b"));
                UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
                UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "maintenance-again").Status, 202);
            }
            f.Outcome(ambiguousFirst ? 1 : 0, NYdb::EStatus::BAD_REQUEST);
            UNIT_ASSERT(f.Store->Read()->PersistentState.PendingMutation);
            f.Failed.insert("b");
            f.Run(6);
            const auto failover = f.Review();
            UNIT_ASSERT_VALUES_EQUAL(failover.Status, 409);
            UNIT_ASSERT_VALUES_EQUAL(failover.Body["error"].GetString(), "PENDING_MUTATION");
            UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), ambiguousFirst ? 2 : 1);
        }
    }

    Y_UNIT_TEST(ManualAmbiguityHasNoAutomaticResendAndRejectionRetainsUncertainty) {
        THarness f(EMode::Manual, {}, 2);
        f.Run(22);
        auto review = f.Review();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "first").Status, 202);
        f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
        f.Run(40);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        UNIT_ASSERT(f.Store->Read()->MayApplyElsewhere);
        review = f.Review();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "second").Status, 202);
        f.Outcome(1, NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT(f.Store->Read()->PersistentState.PendingMutation);
        UNIT_ASSERT(f.Store->Read()->MayApplyElsewhere);
        UNIT_ASSERT_VALUES_EQUAL(f.Review().Body["error"].GetString(), "REJECTED_AT_GENERATION");
    }

    Y_UNIT_TEST(ReceiptEvictionCannotReviveConsumedPlanAndConflictingPayloadIsRejected) {
        THarness f(EMode::Manual);
        f.Run(22);
        const auto review = f.Review();
        const auto id = review.Body["plan_id"].GetString();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(id, "original").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute("other-plan", "original").Body["error"].GetString(), "REQUEST_ID_CONFLICT");
        f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
        for (ui64 n = 0; n < 1025; ++n) {
            TControllerCommand stale;
            stale.Kind = ECommandKind::Mode;
            stale.RequestId = "evict-" + ::ToString(n);
            stale.ExpectedBootId = "old-boot";
            UNIT_ASSERT_VALUES_EQUAL(f.Command(stale).Status, 409);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(id, "original").Body["error"].GetString(), "PLAN_CONSUMED");
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(RefusedInFlightExecuteConsumesReviewAndShutdownClosesDispatch) {
        THarness f(EMode::Manual);
        f.Run(22);
        const auto one = f.Review().Body["plan_id"].GetString();
        const auto two = f.Review().Body["plan_id"].GetString();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(one, "one").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(two, "two").Body["error"].GetString(), "ATTEMPT_IN_FLIGHT");
        f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(two, "two-new").Body["error"].GetString(), "PLAN_CONSUMED");
        const auto three = f.Review().Body["plan_id"].GetString();
        f.Shutdown->store(true);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(three, "three").Status, 503);
        f.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(DuplicateOlderReplyCannotSettleANewEndpointSlot) {
        THarness f;
        f.Run(22);
        f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
        f.Run(30);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 2);
        const auto& first = f.Clients->Updates[0];
        TUpdateResult old;
        old.Rpc = f.Rpc(first.Request, EYdbMethod::Update, first.SentAt);
        old.Plan = first.Plan;
        old.Outcome = EMutationOutcome::Success;
        first.Completed(std::move(old));
        f.Tick();
        UNIT_ASSERT(f.File->GetState().PendingMutation);
        UNIT_ASSERT(!f.Store->Read()->LastMutation->Outcome);
        f.Outcome(1, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT(!f.File->GetState().PendingMutation);
    }

    Y_UNIT_TEST(LastObservedStateRetainsTheLatestSampleRatherThanTheFirstAtGeneration) {
        THarness f(EMode::Observe);
        f.Run(10);
        const auto fresh = f.Store->Read();
        UNIT_ASSERT(fresh->Selection.View && fresh->LastObservedView);
        UNIT_ASSERT(fresh->LastObservedView->SentAt == fresh->Selection.View->SentAt);
        for (const auto& [pile, endpoints] : f.Config.Endpoints) {
            (void)pile;
            for (const auto& endpoint : endpoints) {
                f.Held.insert({endpoint.Uri, EYdbMethod::PlainState});
                f.Held.insert({endpoint.Uri, EYdbMethod::ConnectivityState});
                f.Held.insert({endpoint.Uri, EYdbMethod::SelfCheck});
            }
        }
        f.Run(32);
        UNIT_ASSERT(!f.Store->Read()->Selection.View);
        UNIT_ASSERT(f.Store->Read()->LastObservedView->SentAt == fresh->Selection.View->SentAt);
    }

    Y_UNIT_TEST(ManualAndAutoPublishTheSamePlan) {
        THarness manual(EMode::Manual);
        THarness automatic;
        manual.Run(22);
        automatic.Run(22);
        const auto expected = manual.Store->Read()->Decision;
        UNIT_ASSERT(expected.Kind == EDecisionKind::Ready);
        UNIT_ASSERT(expected.Plan == automatic.Store->Read()->Decision.Plan);
        UNIT_ASSERT_VALUES_EQUAL(expected.CliCommand, automatic.Store->Read()->Decision.CliCommand);
        UNIT_ASSERT(manual.Clients->Updates.empty());
        UNIT_ASSERT_VALUES_EQUAL(automatic.Clients->Updates.size(), 1);
        UNIT_ASSERT(automatic.Clients->Updates.at(0).Plan == *expected.Plan);
    }

    Y_UNIT_TEST(HungSelfCheckNeverReleasesItsSlotOrDelaysTicksAndPolls) {
        THarness fixture;
        fixture.Held.insert({A, EYdbMethod::SelfCheck});
        fixture.Run(40);
        size_t health = 0;
        size_t polls = 0;
        for (const auto& call : fixture.Clients->Health) {
            health += call.Request.Endpoint == A;
        }
        for (const auto& call : fixture.Clients->States) {
            polls += call.Request.Endpoint == A && call.Method == EYdbMethod::PlainState;
        }
        UNIT_ASSERT_VALUES_EQUAL(health, 1);
        UNIT_ASSERT(polls >= 40);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->LastTickAt, fixture.Runtime.GetCurrentMonotonicTime());
    }

    Y_UNIT_TEST(BusySelfCheckSlotAtTheRoundBoundaryStartsWhenItSettles) {
        THarness fixture(EMode::Manual, {}, 1, SlowRounds);
        fixture.Held.insert({A, EYdbMethod::SelfCheck});
        // The first view at 101 starts rounds at 102 and 112; the call sent at 100
        // is still outstanding at both, so only b's endpoint is sampled at 112.
        fixture.Run(12);
        UNIT_ASSERT_VALUES_EQUAL(fixture.CallsAt(EYdbMethod::SelfCheck, TMonotonic::Seconds(112)), 1);
        fixture.Held.clear();
        fixture.Step();
        // The deferred sample goes out as soon as the slot settles, not at the next round.
        UNIT_ASSERT_VALUES_EQUAL(fixture.CallsAt(EYdbMethod::SelfCheck, TMonotonic::Seconds(113)), 1);
        size_t outstanding = 0;
        for (const auto& call : fixture.Clients->Health) {
            outstanding += call.Request.Endpoint == A && !call.Done;
        }
        UNIT_ASSERT(outstanding <= 1);
    }

    Y_UNIT_TEST(GenerationChangeStartsAHealthRound) {
        THarness fixture(EMode::Manual, {}, 1, SlowRounds);
        fixture.Run(4);
        const auto before = fixture.Clients->Health.size();
        fixture.Generation = 8;
        fixture.Step();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 8);
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Tick();
        // The scheduled round is still 4 s away.
        UNIT_ASSERT(fixture.Clients->Health.size() > before);
    }

    Y_UNIT_TEST(FreshRedAndStageTwoEvidenceSurviveFailedAttemptsUntilTtl) {
        THarness fixture(EMode::Manual);
        fixture.Piles["c"] = EPileState::NotSynchronized;
        fixture.Failed.insert("c");
        fixture.Run(3);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("c").Verdict.Verdict == EPileVerdict::Failed);
        const auto sentAt = fixture.Endpoint(A).View->SentAt;
        for (const auto& [pile, endpoints] : fixture.Config.Endpoints) {
            (void)pile;
            for (const auto& endpoint : endpoints) {
                for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
                    fixture.FailedMethods.insert({endpoint.Uri, method});
                }
            }
        }
        fixture.Run(2);
        UNIT_ASSERT(fixture.Store->Read()->Selection.View);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("c").Verdict.Verdict == EPileVerdict::Failed);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Failed);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).View->SentAt, sentAt);
        UNIT_ASSERT(fixture.Endpoint(A).LastError.Contains("TIMEOUT"));
        fixture.Run(31);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoView);
        UNIT_ASSERT(fixture.Store->Read()->Piles.empty());
    }

    Y_UNIT_TEST(FailedAttemptDoesNotEraseFreshContradictoryBscReply) {
        THarness fixture(EMode::Manual);
        fixture.EndpointFailures[A] = {};
        fixture.Run(3);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.FailedMethods.insert({A, EYdbMethod::SelfCheck});
        fixture.Run(2);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Run(31);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Failed);
    }

    Y_UNIT_TEST(PlainReadFailuresOnOneEndpointAreIgnoredWhileTheOthersAnswer) {
        for (const auto status : {NYdb::EStatus::UNAVAILABLE, NYdb::EStatus::NOT_FOUND, NYdb::EStatus::UNSUPPORTED}) {
            THarness fixture(EMode::Manual, {}, 2);
            fixture.FailedStatus = status;
            fixture.FailedMethods = {{A, EYdbMethod::PlainState}, {A, EYdbMethod::ConnectivityState}};
            fixture.Run(22);
            const auto snapshot = fixture.Store->Read();
            UNIT_ASSERT(snapshot->Selection.View);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Selection.View->Generation, 7);
            UNIT_ASSERT(!snapshot->Selection.CurrentEndpoints.contains(A));
            UNIT_ASSERT(snapshot->Selection.CurrentEndpoints.contains(A2));
            UNIT_ASSERT(snapshot->Decision.Kind == EDecisionKind::Ready);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Decision.Plan->Endpoint, A2);
            UNIT_ASSERT(!fixture.Endpoint(A).View);
            UNIT_ASSERT(fixture.Endpoint(A).LastError.StartsWith(SdkStatusName(status)));
            UNIT_ASSERT(fixture.Endpoint(A2).View);
            UNIT_ASSERT(fixture.Endpoint(B).View);
            size_t attempts = 0;
            for (const auto& call : fixture.Clients->States) {
                attempts += call.Request.Endpoint == A && call.Method == EYdbMethod::PlainState;
            }
            UNIT_ASSERT(attempts >= 2);
            UNIT_ASSERT(snapshot->Counters.RpcTotal.at({EYdbMethod::PlainState, SdkStatusName(status)}) >= 2);
        }
    }

    Y_UNIT_TEST(SuccessfulFallbackTruncationAndMismatchReplacePriorGoodEvidence) {
        for (const bool fallback : {false, true}) {
            THarness fixture(EMode::Manual);
            fixture.Failed.clear();
            fixture.Run(22);
            UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Healthy);
            fixture.Fallback = fallback;
            fixture.Truncated = !fallback;
            fixture.Step();
            UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Unknown);
            UNIT_ASSERT(fixture.Endpoint(A).SelfCheck->Classification.Mode == (fallback ? ESelfCheckMode::Fallback : ESelfCheckMode::Bsc));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).SelfCheck->Classification.Truncated, !fallback);
            fixture.Identities[A] = "wrong";
            fixture.Identities[B] = "wrong";
            fixture.Step();
            UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Mismatch);
            // Views of misidentified endpoints are not used.
            UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoView);
        }
    }

    Y_UNIT_TEST(ViewWithoutTheEndpointsOwnPileIsIgnored) {
        THarness fixture(EMode::Manual);
        fixture.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 7);
        // A newer view omitting B's configured pile must not raise high-water,
        // even while the endpoint's pile identity remains VERIFIED.
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Tick();
        for (auto& call : fixture.Clients->States) {
            if (!call.Done && call.Request.Endpoint == B) {
                call.Done = true;
                auto result = fixture.StateReply(call);
                result.View->Generation = 9;
                result.View->Piles.erase("b");
                call.Completed(std::move(result));
            }
        }
        fixture.ReplyReads();
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(B).View->Generation, 9);
        UNIT_ASSERT(fixture.Endpoint(B).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(fixture.Store->Read()->Selection.View);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().GenerationHighWater, 7);
    }

    Y_UNIT_TEST(UnusableConnectivityKeepsValidReportAndRepollsAtNextStatePoll) {
        // 0: binding in progress, usable but not settled; 1: report absent; 2: report
        // from another generation. The settled reports sent at 102 keep Lost until their TTL.
        for (const size_t mode : {0, 1, 2}) {
            THarness fixture(EMode::Manual, {}, 1, SlowRounds);
            fixture.Failed.clear();
            fixture.NoQuorum.insert("b");
            fixture.Run(3);
            const auto lost = [&] {
                const auto snapshot = fixture.Store->Read();
                const auto& pile = snapshot->Piles.at("b");
                return pile.Verdict.Verdict == EPileVerdict::Lost && !pile.Connectivity.empty();
            };
            UNIT_ASSERT(lost());
            fixture.Binding = mode == 0;
            fixture.OmitConnectivity = mode == 1;
            if (mode == 2) {
                fixture.ConnectivityGeneration = 6;
            }
            // The round at 112 returns binding or unusable reports; those sent at 102 stay valid.
            fixture.Run(9);
            UNIT_ASSERT(lost());
            UNIT_ASSERT(fixture.Store->Read()->Selection.View);
            if (mode == 0) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).LastError, "");
            } else {
                UNIT_ASSERT(fixture.Endpoint(A).LastError.StartsWith("CONNECTIVITY_"));
            }
            // The re-poll goes out at the next state poll, not at the next round.
            fixture.Step();
            UNIT_ASSERT(fixture.CallsAt(EYdbMethod::ConnectivityState, TMonotonic::Seconds(113)) > 0);
            UNIT_ASSERT(lost());
            // The retained settled reports still expire at their TTL; binding reports never prove Lost.
            fixture.Run(20);
            const auto snapshot = fixture.Store->Read();
            const auto& pile = snapshot->Piles.at("b");
            UNIT_ASSERT(pile.Verdict.Verdict != EPileVerdict::Lost);
            if (mode == 0) {
                UNIT_ASSERT(!pile.Connectivity.empty());
                for (const auto& report : pile.Connectivity) {
                    UNIT_ASSERT(report.BindingInProgress);
                }
            } else {
                UNIT_ASSERT(pile.Connectivity.empty());
            }
        }
    }

    // Plan step 4.2 "done when": B1 and P1 together. A PRIMARY with BSC, b black-holed,
    // 12 s SelfCheck replies, every connectivity report binding: Ready within
    // warm-up + confirm + one round.
    Y_UNIT_TEST(BlackHoledPileWithSlowSelfCheckAndBindingReportsIsDisconnected) {
        THarness fixture(EMode::Auto, {}, 1, SlowRounds);
        fixture.Binding = true;
        fixture.NoQuorum = {"b"};
        fixture.SelfCheckDelay = TDuration::Seconds(12);
        for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
            fixture.Held.insert({B, method});
        }
        fixture.Run(45);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"b", EPileState::Disconnected}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 7);
        UNIT_ASSERT_VALUES_EQUAL(update.Request.Endpoint, A);
        const auto snapshot = fixture.Store->Read();
        const auto& reports = snapshot->Piles.at("b").Connectivity;
        UNIT_ASSERT(!reports.empty());
        for (const auto& report : reports) {
            UNIT_ASSERT(report.BindingInProgress);
        }
    }

    Y_UNIT_TEST(NewerViewResolvesRestoredPendingInOnePoll) {
        THarness fixture(EMode::Manual, Pending());
        fixture.Generation = 8;
        fixture.Step();
        UNIT_ASSERT(!fixture.File->GetState().PendingMutation);
        UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().GenerationHighWater, 8);
    }

    Y_UNIT_TEST(DifferentPendingUpdateBlocksAndIdenticalRestartResendsOnce) {
        THarness different(EMode::Auto, Pending({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        different.Run(22);
        UNIT_ASSERT(different.Store->Read()->Decision.Blocker == EBlockerReason::PendingMutation);
        UNIT_ASSERT(different.Clients->Updates.empty());
        THarness identical(EMode::Auto, Pending());
        identical.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(identical.Clients->Updates.size(), 1);
        identical.Run(35);
        UNIT_ASSERT_VALUES_EQUAL(identical.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(PendingWriteFailureBlocksUntilAcknowledged) {
        THarness fixture;
        fixture.Run(1);
        fixture.BreakWrites();
        fixture.Run(4);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::StateWriteFailed);
        UNIT_ASSERT(!fixture.Store->Read()->PersistentState.PendingMutation);
        fixture.RestoreWrites();
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(fixture.Store->Read()->PersistentState.PendingMutation);
    }

    Y_UNIT_TEST(FailedPendingWriteLeavesNoUnsentRecord) {
        THarness fixture;
        fixture.Piles["c"] = EPileState::Synchronized;
        fixture.Run(1);
        fixture.BreakWrites();
        fixture.Run(4);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::StateWriteFailed);
        // The plan changes while the unacknowledged record for {b} is retried.
        fixture.Failed.insert("c");
        fixture.Run(4);
        fixture.RestoreWrites();
        fixture.Run(2);
        const TExactUpdate expected = {{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}};
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker != EBlockerReason::PendingMutation);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(fixture.Clients->Updates[0].Plan.Update == expected);
        UNIT_ASSERT(fixture.File->GetState().PendingMutation->Update == expected);
    }

    Y_UNIT_TEST(SuccessAndFailedTerminalWriteBothRejectStaleHistory) {
        for (const bool failWrite : {false, true}) {
            THarness fixture;
            fixture.Run(22);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
            if (failWrite) {
                fixture.BreakWrites();
            }
            fixture.Outcome(0, NYdb::EStatus::SUCCESS);
            fixture.Run(3);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->GenerationHighWater, 8);
            if (failWrite) {
                UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->PersistentState.GenerationHighWater, 7);
                UNIT_ASSERT(fixture.Store->Read()->PersistentState.PendingMutation);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->PendingAttempts, 1);
                UNIT_ASSERT(fixture.Store->Read()->PendingFirstSendUtc);
                UNIT_ASSERT(fixture.Store->Read()->PendingLastSendUtc);
                fixture.RestoreWrites();
                fixture.Tick();
            }
            UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().GenerationHighWater, 8);
            UNIT_ASSERT(!fixture.File->GetState().PendingMutation);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->PendingAttempts, 0);
            UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::StaleHistory);
        }
    }

    Y_UNIT_TEST(ManualResendRequiresAReviewCreatedAfterThePreviousAttempt) {
        THarness f(EMode::Manual);
        f.Run(22);
        const auto first = f.Review().Body["plan_id"].GetString();
        const auto preexisting = f.Review().Body["plan_id"].GetString();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(first, "first-attempt").Status, 202);
        f.Outcome(0, NYdb::EStatus::INTERNAL_ERROR);
        const auto stale = f.Execute(preexisting, "preexisting-review");
        UNIT_ASSERT_VALUES_EQUAL(stale.Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(stale.Body["error"].GetString(), "PLAN_CHANGED");
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        const auto fresh = f.Review().Body["plan_id"].GetString();
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(fresh, "fresh-review").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 2);
    }

    Y_UNIT_TEST(ManualLifecycleCanBeReviewedAgainAfterNotAppliedHereAndFreshPoll) {
        THarness f(EMode::Manual);
        f.Failed.clear();
        f.Run(22);
        const auto first = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(first.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(first.Body["plan_id"].GetString(), "maintenance-first").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates[0].Plan.Endpoint, A);
        f.Outcome(0, NYdb::EStatus::UNAVAILABLE);
        f.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1); // Manual never resends itself.
        const auto second = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(second.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(second.Body["plan_id"].GetString(), "maintenance-second").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 2);
        UNIT_ASSERT(f.Clients->Updates[1].Plan.Update == (TExactUpdate{{"b", EPileState::Suspended}}));
    }

    Y_UNIT_TEST(ManualLifecycleKeepsNotAppliedHereExclusionsAcrossPollsUntilExhausted) {
        THarness f(EMode::Manual, {}, 2);
        f.Failed.clear();
        f.Run(22);
        auto review = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "first").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates[0].Plan.Endpoint, A);
        f.Outcome(0, NYdb::EStatus::UNAVAILABLE);
        f.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 1);
        review = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "alternative").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates[1].Plan.Endpoint, A2);
        f.Outcome(1, NYdb::EStatus::PRECONDITION_FAILED);
        f.Run(3);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates.size(), 2);
        review = f.Review(EOperationKind::TakeOffline, TString("b"));
        UNIT_ASSERT_VALUES_EQUAL(review.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(f.Execute(review.Body["plan_id"].GetString(), "exhausted").Status, 202);
        UNIT_ASSERT_VALUES_EQUAL(f.Clients->Updates[2].Plan.Endpoint, A);
        for (const auto& call : f.Clients->Updates) {
            UNIT_ASSERT(call.Plan.Update == (TExactUpdate{{"b", EPileState::Suspended}}));
        }
    }

    Y_UNIT_TEST(NotAppliedHereImmediatelyUsesAnotherQualifiedEndpoint) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[0].Plan.Endpoint, A);
        fixture.Outcome(0, NYdb::EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A2);
        UNIT_ASSERT(fixture.Clients->Updates[0].Plan.Update == fixture.Clients->Updates[1].Plan.Update);
    }

    Y_UNIT_TEST(IdenticalRetryDoesNotRewriteDurablePendingRecord) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto pending = fixture.File->GetState().PendingMutation;
        UNIT_ASSERT(pending);
        fixture.File->SetDirectorySyncFailureForTests([] {
            return true;
        });
        fixture.Outcome(0, NYdb::EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[0].Plan.Endpoint, A);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A2);
        UNIT_ASSERT(fixture.Clients->Updates[1].Plan.Update == pending->Update);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.ExpectedGeneration, pending->ExpectedGeneration);
        UNIT_ASSERT(fixture.File->GetState().PendingMutation == pending);
        UNIT_ASSERT(fixture.Store->Read()->PersistentState.PendingMutation == pending);
        UNIT_ASSERT(fixture.Store->Read()->StateWriteError.empty());
    }

    Y_UNIT_TEST(NotAppliedHereUsesAnotherEndpointAtStatePoll) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);

        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Outcome(0, NYdb::EStatus::UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[0].Plan.Endpoint, A);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A2);
        UNIT_ASSERT(fixture.Clients->Updates[0].Plan.Update == fixture.Clients->Updates[1].Plan.Update);
    }

    Y_UNIT_TEST(NotAppliedHereRetriesOnlyQualifiedEndpointAfterStatePoll) {
        THarness fixture;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);

        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Outcome(0, NYdb::EStatus::UNAVAILABLE);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);

        fixture.Run(1);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A);
    }

    Y_UNIT_TEST(TwoNotAppliedHereEndpointsAreRetriedOncePerStatePoll) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[0].Plan.Endpoint, A);
        fixture.Outcome(0, NYdb::EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A2);
        fixture.Outcome(1, NYdb::EStatus::UNAVAILABLE);

        // Both endpoints stay excluded within this state-poll interval.
        for (size_t i = 0; i < 3; ++i) {
            fixture.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(250));
            fixture.Tick();
            fixture.ReplyReads();
            fixture.Tick();
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);

        // The next state poll finds no selectable endpoint and clears the exclusion.
        fixture.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(250));
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 3);
        UNIT_ASSERT(fixture.Clients->Updates[2].Plan.Update == fixture.Clients->Updates[0].Plan.Update);
    }

    Y_UNIT_TEST(RefusedCommandDoesNotDropUnavailableAutoRoute) {
        THarness fixture;
        fixture.Piles["c"] = EPileState::Synchronized;
        fixture.Run(22);
        fixture.Generation = 8;
        fixture.Run(23);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);
        const auto decisions = fixture.Logs("decision").size();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Review().Status, 409);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Logs("decision").size(), decisions);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(RefusedCommandAndModeChangesKeepRejectedDecision) {
        THarness fixture;
        fixture.Run(22);
        fixture.Outcome(0, NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Decision.Reason, "REJECTED_BY_SERVER");
        const auto decisions = fixture.Logs("decision").size();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Review().Status, 409);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Decision.Reason, "REJECTED_BY_SERVER");
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Logs("decision").size(), decisions);
        for (const auto mode : {EMode::Manual, EMode::Observe, EMode::Auto}) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Mode(mode, "rejected-mode-" + ToString(mode)).Status, 200);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Decision.Reason, "REJECTED_BY_SERVER");
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(AmbiguousBackoffAndRejectedRestartBehavior) {
        THarness ambiguous;
        ambiguous.Run(22);
        ambiguous.Outcome(0, NYdb::EStatus::TIMEOUT);
        ambiguous.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Clients->Updates.size(), 1);
        ambiguous.Run(30);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Store->Read()->LastMutation->AmbiguousResends, 1);
        ambiguous.Outcome(1, NYdb::EStatus::TIMEOUT);
        ambiguous.Run(25);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Clients->Updates.size(), 2);
        ambiguous.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Clients->Updates.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ambiguous.Store->Read()->LastMutation->AmbiguousResends, 2);
        THarness rejected;
        rejected.Run(22);
        rejected.Outcome(0, NYdb::EStatus::BAD_REQUEST);
        rejected.Run(40);
        UNIT_ASSERT_VALUES_EQUAL(rejected.Clients->Updates.size(), 1);
        THarness restart(EMode::Auto, rejected.File->GetState());
        restart.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(restart.Clients->Updates.size(), 1);
    }

    Y_UNIT_TEST(RejectedRetryCannotForgetAmbiguousAttemptAcrossRestart) {
        THarness fixture;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto original = fixture.Clients->Updates.front().Plan.Update;
        fixture.Outcome(0, NYdb::EStatus::TIMEOUT);
        fixture.Run(30);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT(fixture.Clients->Updates.back().Plan.Update == original);
        fixture.Outcome(1, NYdb::EStatus::UNAUTHORIZED);
        fixture.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);

        // The first invocation can still commit after its client timed out.
        // Restart with observations that would instead promote b at the same G.
        THarness restart(EMode::Auto, fixture.File->GetState());
        restart.Failed = {"a"};
        restart.Run(25);
        UNIT_ASSERT(restart.Clients->Updates.empty());
        UNIT_ASSERT(restart.Store->Read()->Decision.Blocker == EBlockerReason::PendingMutation);
        UNIT_ASSERT(restart.File->GetState().PendingMutation->Update == original);
    }

    Y_UNIT_TEST(RejectedSupersetCannotForgetOutstandingAttemptAcrossRestart) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Piles["c"] = EPileState::Synchronized;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        fixture.Failed.insert("c");
        fixture.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT(!fixture.Clients->Updates.front().Done);
        const auto superset = fixture.Clients->Updates.back().Plan.Update;
        fixture.Outcome(1, NYdb::EStatus::BAD_REQUEST);

        THarness restart(EMode::Auto, fixture.File->GetState());
        restart.Piles["c"] = EPileState::Synchronized;
        restart.Failed = {"a"};
        restart.Run(25);
        UNIT_ASSERT(restart.Clients->Updates.empty());
        UNIT_ASSERT(restart.Store->Read()->Decision.Blocker == EBlockerReason::PendingMutation);
        UNIT_ASSERT(restart.File->GetState().PendingMutation->Update == superset);
    }

    Y_UNIT_TEST(NoPendingIsCreatedUntilASurvivingDispatchSlotExists) {
        THarness fixture;
        fixture.Piles["c"] = EPileState::Synchronized;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        fixture.Generation = 8;
        fixture.Run(23);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(!fixture.File->GetState().PendingMutation);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoMutationEndpoint);
        // A new failure at this same generation changes the unsent plan. It
        // must not be frozen by a record created without a dispatch slot.
        fixture.Failed.insert("c");
        fixture.Run(2);
        UNIT_ASSERT(!fixture.File->GetState().PendingMutation);
        fixture.Outcome(0, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.back().Plan.ExpectedGeneration, 8);
        UNIT_ASSERT(fixture.Clients->Updates.back().Plan.Update.contains("b"));
        UNIT_ASSERT(fixture.Clients->Updates.back().Plan.Update.contains("c"));
        UNIT_ASSERT(fixture.File->GetState().PendingMutation->Update == fixture.Clients->Updates.back().Plan.Update);
    }

    Y_UNIT_TEST(OlderOutcomeCannotClearNewerPending) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(22);
        fixture.Generation = 8;
        fixture.Run(23);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().PendingMutation->ExpectedGeneration, 8);
        fixture.Outcome(0, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().PendingMutation->ExpectedGeneration, 8);
        UNIT_ASSERT_VALUES_EQUAL(fixture.File->GetState().GenerationHighWater, 8);
    }

    // B2 (D2): a same-primary superset plan replaces the unresolved record at G.
    Y_UNIT_TEST(SupersetPlanReplacesThePendingRecordAtTheSameGeneration) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Piles["c"] = EPileState::Synchronized;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(fixture.Clients->Updates[0].Plan.Update == TExactUpdate({{"b", EPileState::Disconnected}}));
        // c fails while the first invocation is still outstanding on A.
        fixture.Failed.insert("c");
        fixture.Run(5);
        const TExactUpdate expected = {{"b", EPileState::Disconnected}, {"c", EPileState::Disconnected}};
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT(fixture.Clients->Updates[1].Plan.Update == expected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.ExpectedGeneration, 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[1].Plan.Endpoint, A2);
        UNIT_ASSERT(fixture.File->GetState().PendingMutation->Update == expected);
        // The superseded invocation's outcome is report-only.
        fixture.Outcome(0, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT(fixture.File->GetState().PendingMutation->Update == expected);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
    }

    Y_UNIT_TEST(LateConnectivityAndOptionalFailureDoNotReplaceNewerPlainView) {
        THarness fixture(EMode::Manual);
        fixture.Held.insert({A, EYdbMethod::ConnectivityState});
        fixture.Step();
        size_t heldIndex = 0;
        while (heldIndex < fixture.Clients->States.size()
            && (fixture.Clients->States[heldIndex].Request.Endpoint != A || fixture.Clients->States[heldIndex].Method != EYdbMethod::ConnectivityState)) {
            ++heldIndex;
        }
        const auto old = fixture.StateReply(fixture.Clients->States.at(heldIndex));
        fixture.Generation = 8;
        fixture.Step();
        fixture.Clients->States.at(heldIndex).Done = true;
        fixture.Clients->States.at(heldIndex).Completed(old);
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).View->Generation, 8);
        fixture.Held.erase({A, EYdbMethod::ConnectivityState});
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Tick();
        for (auto& call : fixture.Clients->States) {
            if (!call.Done && call.Request.Endpoint == A && call.Method == EYdbMethod::ConnectivityState) {
                call.Done = true;
                TStateResult failed;
                failed.Rpc = fixture.Rpc(call.Request, call.Method, call.SentAt);
                failed.Rpc.Success = false;
                failed.Rpc.Status = "UNSUPPORTED";
                failed.Error = "UNSUPPORTED";
                call.Completed(std::move(failed));
            }
        }
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).View->Generation, 8);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 8);
    }

    Y_UNIT_TEST(ActualCallerExcludesStaleEndpointDespiteMatchingGeneration) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Held.insert({A, EYdbMethod::PlainState});
        fixture.Held.insert({A, EYdbMethod::ConnectivityState});
        fixture.Held.insert({A, EYdbMethod::SelfCheck});
        // Seed A at G, then leave its state methods outstanding. Its identity
        // becomes verified only after that view expires, while A2 stays fresh.
        for (auto& call : fixture.Clients->States) {
            if (!call.Done && call.Request.Endpoint == A) {
                call.Done = true;
                call.Completed(fixture.StateReply(call));
            }
        }
        fixture.Run(35);
        fixture.Held.erase({A, EYdbMethod::SelfCheck});
        fixture.Step();
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(A).View->Generation, fixture.Generation);
        UNIT_ASSERT(fixture.Runtime.GetCurrentMonotonicTime() - fixture.Endpoint(A).View->SentAt > fixture.Config.Timing.EvidenceTtl);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Plan);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Decision.Plan->Endpoint, A2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.front().Plan.Endpoint, A2);
    }

    Y_UNIT_TEST(FreshIncompatibleEndpointViewsBlockActualDispatch) {
        THarness fixture(EMode::Auto, {}, 2);
        fixture.Run(1);
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Tick();
        for (auto& call : fixture.Clients->States) {
            if (!call.Done && call.Request.Endpoint == A2) {
                call.Done = true;
                auto result = fixture.StateReply(call);
                result.View->Piles["b"] = EPileState::NotSynchronized;
                call.Completed(std::move(result));
            }
        }
        fixture.ReplyReads();
        fixture.Tick();
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::ViewConflict);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
    }

    Y_UNIT_TEST(MismatchedAndPileLessEndpointsKeepTheirIdentityRules) {
        THarness fixture(EMode::Manual);
        fixture.Identities[A] = "wrong";
        fixture.Run(3);
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Mismatch);
        fixture.Identities.erase(A);
        fixture.Run(3);
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Verified);
        // A node whose storage config is not ready answers without a pile.
        fixture.PileLess.insert(A);
        fixture.Run(3);
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(fixture.Endpoint(A).LastError.Contains("ENDPOINT_PILE_UNVERIFIED"));
    }

    Y_UNIT_TEST(PileLessSelfCheckKeepsVerifiedIdentityAndEligibility) {
        THarness fixture;
        fixture.Run(3);
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Verified);
        // A node whose storage config is not ready answers without a pile.
        fixture.PileLess.insert(A);
        fixture.Run(22);
        UNIT_ASSERT(fixture.Endpoint(A).Identity == EEndpointIdentity::Verified);
        UNIT_ASSERT(fixture.Endpoint(A).LastError.Contains("ENDPOINT_PILE_UNVERIFIED"));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates[0].Plan.Endpoint, A);
    }

    Y_UNIT_TEST(NoViewBreaksWarmupAndDownContinuity) {
        THarness fixture(EMode::Manual);
        fixture.Run(22);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
        for (const auto& [pile, endpoints] : fixture.Config.Endpoints) {
            (void)pile;
            for (const auto& endpoint : endpoints) {
                for (const auto method : {EYdbMethod::PlainState, EYdbMethod::ConnectivityState, EYdbMethod::SelfCheck}) {
                    fixture.Held.insert({endpoint.Uri, method});
                }
            }
        }
        fixture.Run(32);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoView);
        fixture.Held.clear();
        fixture.Run(2);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind != EDecisionKind::Ready);
        fixture.Run(21);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
    }

    // Warm-up, confirmation and the schedule on one timeline: a role change at
    // t = 10 restarts the survivor's clean window, so the primary's loss at t = 20,
    // confirmed by t = 26, is acted on only at t = 40, when b becomes Healthy.
    Y_UNIT_TEST(FailoverWaitsForTheWarmupAfterARoleChangeAndForConfirmation) {
        THarness fixture(EMode::Auto, {}, 1, "{round_interval_seconds: 1, confirm_seconds: 5, bsc_warmup_seconds: 30, read_timeout_seconds: 5, selfcheck_operation_timeout_seconds: 12, evidence_ttl_seconds: 30}");
        fixture.Piles = {{"a", EPileState::Synchronized}, {"b", EPileState::Primary}};
        fixture.Failed.clear();
        fixture.Run(10);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Unknown);
        fixture.Piles = {{"a", EPileState::Primary}, {"b", EPileState::Synchronized}};
        fixture.Generation = 8;
        fixture.Run(10);
        fixture.Failed = {"a"};
        fixture.Run(10);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoCandidate);
        fixture.Run(8);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
        fixture.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto& update = fixture.Clients->Updates.front();
        UNIT_ASSERT(update.SentAt >= TMonotonic::Seconds(140));
        UNIT_ASSERT(update.Plan.Update == TExactUpdate({{"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        UNIT_ASSERT_VALUES_EQUAL(update.Plan.ExpectedGeneration, 8);
    }

    Y_UNIT_TEST(RoleChangeWarmupDoesNotReuseCachedHealthAfterFailedCalls) {
        THarness fixture(EMode::Manual);
        fixture.Failed.clear();
        fixture.ReplyReads();
        fixture.Tick();
        fixture.FailedMethods.insert({A, EYdbMethod::SelfCheck});
        fixture.FailedMethods.insert({B, EYdbMethod::SelfCheck});
        fixture.Run(28);
        fixture.Piles = {{"a", EPileState::Synchronized}, {"b", EPileState::Primary}};
        fixture.Generation = 8;
        fixture.Step();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 8);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.FailedMethods.clear();
        fixture.Step();
        // The first new clean send at 130 cannot extend the cached send at 100
        // through the role change observed at 129. The configured warm-up is 20 s.
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Run(19);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Run(2);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(SuccessfulRoleChangeKeepsWarmupHistoryThroughStaleSelection) {
        THarness fixture(EMode::Auto, {}, 1,
            FastRoundsWith30SecondWarmup);
        fixture.Failed = {"a"};
        fixture.Run(32);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        UNIT_ASSERT(fixture.Clients->Updates.front().Plan.Update == TExactUpdate({
            {"a", EPileState::Disconnected}, {"b", EPileState::Primary}}));
        const auto cachedSend = fixture.Endpoint(B).SelfCheck->SentAt;
        fixture.FailedMethods.insert({A, EYdbMethod::SelfCheck});
        fixture.FailedMethods.insert({B, EYdbMethod::SelfCheck});
        fixture.Run(2);
        fixture.Outcome(0, NYdb::EStatus::SUCCESS);
        // Success raises the floor before independently polled state catches up.
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::StaleHistory);
        UNIT_ASSERT(!fixture.Store->Read()->Selection.View);
        fixture.Piles = {{"a", EPileState::Disconnected}, {"b", EPileState::Primary}};
        fixture.Generation = 8;
        fixture.Step();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Selection.View->Generation, 8);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(B).SelfCheck->SentAt, cachedSend);
        const auto firstCleanSend = cachedSend + fixture.Config.Timing.BscWarmup;
        while (fixture.Runtime.GetCurrentMonotonicTime() + TDuration::Seconds(1) < firstCleanSend) {
            fixture.Step();
        }
        fixture.FailedMethods.clear();
        fixture.Step();
        const auto firstPostChangeSend = fixture.Endpoint(B).SelfCheck->SentAt;
        // A call launched at the preceding tick may settle first. Also settle
        // the deferred sample launched now, so the old window would span 30 s.
        fixture.ReplyReads();
        fixture.Tick();
        UNIT_ASSERT(fixture.Endpoint(B).SelfCheck->SentAt >= firstCleanSend);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        while (fixture.Runtime.GetCurrentMonotonicTime() + TDuration::Seconds(1)
            < firstPostChangeSend + fixture.Config.Timing.BscWarmup)
        {
            fixture.Step();
        }
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Step();
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(ViewConflictRecoveryPreservesAnEstablishedRoleBoundary) {
        THarness fixture(EMode::Manual, {}, 1,
            FastRoundsWith30SecondWarmup);
        fixture.Failed.clear();
        fixture.ReplyReads();
        fixture.Tick();
        fixture.FailedMethods.insert({A, EYdbMethod::SelfCheck});
        fixture.FailedMethods.insert({B, EYdbMethod::SelfCheck});
        fixture.Run(24);
        fixture.Piles["b"] = EPileState::Promoted;
        fixture.Generation = 8;
        fixture.Step();
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        fixture.Tick();
        for (auto& call : fixture.Clients->States) {
            if (!call.Done && call.Request.Endpoint == A) {
                call.Done = true;
                auto result = fixture.StateReply(call);
                result.View->Piles["b"] = EPileState::Synchronized;
                call.Completed(std::move(result));
            }
        }
        fixture.ReplyReads();
        fixture.Tick();
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::ViewConflict);
        fixture.Run(3);
        UNIT_ASSERT(fixture.Store->Read()->Selection.View);
        fixture.FailedMethods.clear();
        fixture.Step();
        // Recovery has the same roles as before the conflict. The boundary at
        // 125 must still exclude the cached send at 100 from this new window.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Endpoint(B).SelfCheck->SentAt, TMonotonic::Seconds(130));
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Run(29);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Unknown);
        fixture.Run(1);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(InitialViewDelayPreservesStartupHealthSendTimes) {
        THarness fixture(EMode::Manual, {}, 1,
            FastRoundsWith30SecondWarmup);
        fixture.Failed.clear();
        for (const auto& endpoint : {A, B}) {
            fixture.Held.insert({endpoint, EYdbMethod::PlainState});
            fixture.Held.insert({endpoint, EYdbMethod::ConnectivityState});
        }
        fixture.ReplyReads();
        fixture.Tick();
        fixture.FailedMethods.insert({A, EYdbMethod::SelfCheck});
        fixture.FailedMethods.insert({B, EYdbMethod::SelfCheck});
        fixture.Run(12);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Blocker == EBlockerReason::NoView);
        fixture.Held.clear();
        fixture.Run(17);
        UNIT_ASSERT(fixture.Store->Read()->Selection.View);
        fixture.FailedMethods.clear();
        fixture.Step();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GetCurrentMonotonicTime(), TMonotonic::Seconds(130));
        // There was no previously established role: genuine startup still uses
        // the initial send at 100 when its first view arrives at 113.
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("b").Verdict.Verdict == EPileVerdict::Healthy);
    }

    Y_UNIT_TEST(StopAcknowledgesBeforeLateCompletionsAndPreventsNewCalls) {
        THarness fixture;
        const auto count = fixture.Clients->States.size() + fixture.Clients->Health.size();
        fixture.Runtime.SendAsync(new NActors::IEventHandle(fixture.Actor, fixture.Edge, new TEvSkipper::TEvStop()));
        fixture.Runtime.GrabEdgeEvent<TEvSkipper::TEvStopped>(fixture.Edge);
        fixture.ReplyReads();
        fixture.Run(5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->States.size() + fixture.Clients->Health.size(), count);
        UNIT_ASSERT(fixture.Clients->Updates.empty());
    }

    // S14 (B1): slow SelfCheck replies do not starve the BSC warm-up.
    Y_UNIT_TEST(SlowSelfCheckRepliesDoNotStarveTheWarmup) {
        THarness fixture(EMode::Manual, {}, 1, "{round_interval_seconds: 10, confirm_seconds: 10, bsc_warmup_seconds: 30, read_timeout_seconds: 5, selfcheck_operation_timeout_seconds: 12, evidence_ttl_seconds: 30}");
        fixture.Failed.clear();
        fixture.SelfCheckDelay = TDuration::Seconds(12);
        fixture.Run(50);
        UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Healthy);
        for (size_t i = 0; i < 60; ++i) {
            fixture.Step();
            UNIT_ASSERT(fixture.Store->Read()->Piles.at("a").Verdict.Verdict == EPileVerdict::Healthy);
        }
    }

    // A rejection stops Auto locally while preserving the durable same-G overlay.
    Y_UNIT_TEST(RejectedOutcomePreservesThePendingLockAndStopsAutoUntilNextGeneration) {
        THarness fixture;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        fixture.Outcome(0, NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT(fixture.File->GetState().PendingMutation);
        fixture.Run(40);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        const auto decision = fixture.Store->Read()->Decision;
        UNIT_ASSERT(decision.Kind == EDecisionKind::Ready);
        UNIT_ASSERT(decision.Reason.Contains("REJECTED_BY_SERVER"));
        UNIT_ASSERT(!decision.CliCommand.empty());
        // The next generation is a new decision and is sent.
        fixture.Generation = 8;
        fixture.Run(25);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.back().Plan.ExpectedGeneration, 8);
    }
}

Y_UNIT_TEST_SUITE(TSkipperObservability) {
    Y_UNIT_TEST(HistoryInternalEventsHaveExplicitOrigins) {
        THarness fixture(EMode::Observe);
        fixture.Failed.clear();
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Mode(EMode::Manual).Status, 200);
        bool mode = false;
        bool decision = false;
        const auto history = fixture.Store->Read()->Diagnostics->ReadHistory();
        for (const auto& row : history["rows"].GetArray()) {
            if (row["type"].GetString() == "mode") {
                mode = true;
                UNIT_ASSERT_VALUES_EQUAL(row["origin"].GetString(), "operator");
                UNIT_ASSERT_VALUES_EQUAL(row["origin_label"].GetString(), "Operator");
            } else if (row["type"].GetString() == "decision") {
                decision = true;
                UNIT_ASSERT_VALUES_EQUAL(row["origin"].GetString(), "skipper");
                UNIT_ASSERT_VALUES_EQUAL(row["origin_label"].GetString(), "Skipper");
            }
        }
        UNIT_ASSERT(mode && decision);
    }

    Y_UNIT_TEST(HistoryNamesTheUnobservedGenerationRange) {
        THarness fixture(EMode::Observe);
        fixture.Generation = 42;
        fixture.Failed.clear();
        fixture.Run(22);
        fixture.Generation = 45;
        fixture.Run(1);
        bool found = false;
        const auto history = fixture.Store->Read()->Diagnostics->ReadHistory();
        for (const auto& row : history["rows"].GetArray()) {
            if (row["type"].GetString() == "view" && row["generation"].GetString() == "45") {
                found = true;
                UNIT_ASSERT(row["coverage_gap"].GetBoolean());
                UNIT_ASSERT_VALUES_EQUAL(row["coverage_gap_text"].GetString(), "Generations 43–44 not observed.");
                UNIT_ASSERT_STRING_CONTAINS(row["summary"].GetString(), "Generations 43–44 not observed.");
            }
        }
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST(DecisionChangesAreLogged) {
        THarness fixture(EMode::Manual);
        fixture.Run(22);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
        const auto decisions = fixture.Logs("decision");
        UNIT_ASSERT(!decisions.empty());
        UNIT_ASSERT_VALUES_EQUAL(decisions.back()["kind"].GetString(), "READY");
    }

    Y_UNIT_TEST(DecisionIsLoggedOncePerChange) {
        THarness fixture(EMode::Manual);
        fixture.Run(22);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
        const auto count = fixture.Logs("decision").size();
        fixture.Run(3);
        UNIT_ASSERT(fixture.Store->Read()->Decision.Kind == EDecisionKind::Ready);
        const auto decisions = fixture.Logs("decision");
        UNIT_ASSERT_VALUES_EQUAL(decisions.size(), count);
        size_t ready = 0;
        for (const auto& record : decisions) {
            ready += record["kind"].GetString() == "READY";
        }
        UNIT_ASSERT_VALUES_EQUAL(ready, 1);
    }

    Y_UNIT_TEST(ViewChangesLogInitialOnceAndWarnWhenUnexplained) {
        THarness fixture(EMode::Manual);
        fixture.Failed.clear();
        fixture.Step();
        auto views = fixture.Logs("view_change");
        UNIT_ASSERT_VALUES_EQUAL(views.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(views[0]["priority"].GetString(), "INFO");
        UNIT_ASSERT_VALUES_EQUAL(views[0]["explanation"].GetString(), "Initial");
        UNIT_ASSERT_VALUES_EQUAL(Json(views[0]["states"].GetString())["b"].GetString(), "SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Counters.UnexplainedStateChanges, 0);
        fixture.Piles["b"] = EPileState::Disconnected;
        ++fixture.Generation;
        fixture.Step();
        fixture.Run(2);
        views = fixture.Logs("view_change");
        UNIT_ASSERT_VALUES_EQUAL(views.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(views[1]["priority"].GetString(), "WARN");
        UNIT_ASSERT_VALUES_EQUAL(views[1]["explanation"].GetString(), "Unexplained");
        UNIT_ASSERT_VALUES_EQUAL(Json(views[1]["states"].GetString())["b"].GetString(), "DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Counters.UnexplainedStateChanges, 1);
        // A generation jump with unchanged piles is explained.
        fixture.Generation += 2;
        fixture.Step();
        views = fixture.Logs("view_change");
        UNIT_ASSERT_VALUES_EQUAL(views.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(views.back()["explanation"].GetString(), "Explained");
        UNIT_ASSERT_VALUES_EQUAL(views.back()["priority"].GetString(), "INFO");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Counters.UnexplainedStateChanges, 1);
    }

    Y_UNIT_TEST(OwnSuccessfulMutationIsExplained) {
        THarness fixture;
        fixture.Run(22);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Clients->Updates.size(), 1);
        fixture.Outcome(0, NYdb::EStatus::SUCCESS);
        UNIT_ASSERT(!fixture.File->GetState().PendingMutation);
        fixture.Generation = 8;
        fixture.Piles["b"] = EPileState::Disconnected;
        fixture.Step();
        const auto views = fixture.Logs("view_change");
        UNIT_ASSERT_VALUES_EQUAL(views.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(views.back()["explanation"].GetString(), "Explained");
        UNIT_ASSERT_VALUES_EQUAL(views.back()["priority"].GetString(), "INFO");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Store->Read()->Counters.UnexplainedStateChanges, 0);
    }
}

} // namespace NBridgeSkipper
