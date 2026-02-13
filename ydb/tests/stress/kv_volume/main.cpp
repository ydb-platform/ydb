#include <ydb/tests/stress/kv_volume/protos/config.pb.h>

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_keyvalue.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/getopt/last_getopt.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>

namespace {

using namespace std::chrono_literals;

constexpr ui16 DefaultKvPort = 2135;
constexpr ui32 GrpcRetryCount = 10;
constexpr auto GrpcRetrySleep = 100ms;
constexpr auto GrpcDeadline = 10s;

const TString DataPattern = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

struct TOptions {
    TString Endpoint = "grpc://localhost:2135";
    TString Database;
    ui32 Duration = 120;
    ui32 InFlight = 1;
    TString Version = "v1";
    TString ConfigPath;
    TString ConfigName;
    bool AllowErrors = false;
    bool Verbose = false;
};

struct TKeyInfo {
    ui32 PartitionId = 0;
    ui32 KeySize = 0;
};

std::mt19937_64& RandomEngine() {
    thread_local std::mt19937_64 engine(std::random_device{}());
    return engine;
}

TString StatusToString(Ydb::StatusIds::StatusCode status) {
    return Ydb::StatusIds::StatusCode_Name(status);
}

TString MakeVolumePath(const TString& database, const TString& path) {
    if (database.empty()) {
        return path;
    }
    if (database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

TString ParseHostPort(const TString& endpoint) {
    TString hostPort = endpoint;
    const TString scheme = "://";
    const size_t pos = hostPort.find(scheme);
    if (pos != TString::npos) {
        hostPort = hostPort.substr(pos + scheme.size());
    }

    if (hostPort.empty()) {
        return TStringBuilder() << "localhost:" << DefaultKvPort;
    }

    if (hostPort.find(':') == TString::npos) {
        hostPort += TStringBuilder() << ":" << DefaultKvPort;
    }

    return hostPort;
}

TString GeneratePatternData(ui32 size) {
    if (!size) {
        return {};
    }
    const size_t repeats = (size + DataPattern.size() - 1) / DataPattern.size();
    TString result;
    result.reserve(repeats * DataPattern.size());
    for (size_t i = 0; i < repeats; ++i) {
        result += DataPattern;
    }
    result.resize(size);
    return result;
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

class TRunStats {
public:
    void RecordAction(const TString& actionName) {
        std::lock_guard lock(Mutex_);
        ++ActionRuns_[actionName];
    }

    void RecordError(const TString& kind, const TString& message) {
        std::lock_guard lock(Mutex_);
        ++ErrorsByKind_[kind];
        ++TotalErrors_;

        if (SampleErrors_.size() < 20) {
            SampleErrors_.push_back(TStringBuilder() << kind << ": " << message);
        }
    }

    ui64 GetTotalErrors() const {
        std::lock_guard lock(Mutex_);
        return TotalErrors_;
    }

    void PrintSummary() const {
        THashMap<TString, ui64> actionRuns;
        THashMap<TString, ui64> errorsByKind;
        TVector<TString> sampleErrors;
        ui64 totalErrors = 0;

        {
            std::lock_guard lock(Mutex_);
            actionRuns = ActionRuns_;
            errorsByKind = ErrorsByKind_;
            sampleErrors = SampleErrors_;
            totalErrors = TotalErrors_;
        }

        TVector<std::pair<TString, ui64>> sortedActions(actionRuns.begin(), actionRuns.end());
        std::sort(sortedActions.begin(), sortedActions.end(), [](const auto& l, const auto& r) {
            return l.first < r.first;
        });

        TVector<std::pair<TString, ui64>> sortedErrors(errorsByKind.begin(), errorsByKind.end());
        std::sort(sortedErrors.begin(), sortedErrors.end(), [](const auto& l, const auto& r) {
            return l.first < r.first;
        });

        Cout << "==== kv_volume summary ====" << Endl;
        Cout << "Action runs:" << Endl;
        for (const auto& [name, count] : sortedActions) {
            Cout << "  " << name << ": " << count << Endl;
        }

        Cout << "Errors by kind:" << Endl;
        if (sortedErrors.empty()) {
            Cout << "  none" << Endl;
        } else {
            for (const auto& [name, count] : sortedErrors) {
                Cout << "  " << name << ": " << count << Endl;
            }
        }

        Cout << "Total errors: " << totalErrors << Endl;

        if (!sampleErrors.empty()) {
            Cout << "Sample errors:" << Endl;
            for (const auto& error : sampleErrors) {
                Cout << "  " << error << Endl;
            }
        }
    }

private:
    mutable std::mutex Mutex_;
    THashMap<TString, ui64> ActionRuns_;
    THashMap<TString, ui64> ErrorsByKind_;
    TVector<TString> SampleErrors_;
    ui64 TotalErrors_ = 0;
};

class TDataStorage {
public:
    void AddKey(const TString& actionName, const TString& key, const TKeyInfo& keyInfo) {
        std::lock_guard lock(Mutex_);
        KeysByAction_[actionName][key] = keyInfo;
    }

    TVector<std::pair<TString, TKeyInfo>> PickKeys(const TVector<TString>& actionNames, ui32 count, bool erase) {
        std::lock_guard lock(Mutex_);

        TVector<std::pair<TString, TKeyInfo>> candidates;
        THashSet<TString> seen;
        for (const TString& actionName : actionNames) {
            const auto actionIt = KeysByAction_.find(actionName);
            if (actionIt == KeysByAction_.end()) {
                continue;
            }

            for (const auto& [key, info] : actionIt->second) {
                if (seen.insert(key).second) {
                    candidates.emplace_back(key, info);
                }
            }
        }

        if (candidates.empty()) {
            return {};
        }

        std::shuffle(candidates.begin(), candidates.end(), RandomEngine());
        const ui32 limit = std::min<ui32>(count, candidates.size());
        candidates.resize(limit);

        if (erase) {
            for (const auto& [key, _] : candidates) {
                for (const TString& actionName : actionNames) {
                    auto actionIt = KeysByAction_.find(actionName);
                    if (actionIt == KeysByAction_.end()) {
                        continue;
                    }
                    if (actionIt->second.erase(key)) {
                        break;
                    }
                }
            }
        }

        return candidates;
    }

private:
    std::mutex Mutex_;
    THashMap<TString, THashMap<TString, TKeyInfo>> KeysByAction_;
};

class TKeyValueClientV1 {
public:
    explicit TKeyValueClientV1(const TString& hostPort)
        : Channel_(grpc::CreateChannel(hostPort, grpc::InsecureChannelCredentials()))
        , Stub_(Ydb::KeyValue::V1::KeyValueService::NewStub(Channel_))
    {
    }

    bool CreateVolume(const TString& path, ui32 partitionCount, const TVector<TString>& channels, TString* error) {
        Ydb::KeyValue::CreateVolumeRequest request;
        request.set_path(path);
        request.set_partition_count(partitionCount);
        for (const TString& channelMedia : channels) {
            request.mutable_storage_config()->add_channel()->set_media(channelMedia);
        }

        Ydb::KeyValue::CreateVolumeResponse response;
        if (!CallWithRetry([&](grpc::ClientContext* ctx) { return Stub_->CreateVolume(ctx, request, &response); }, error)) {
            return false;
        }

        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            *error = TStringBuilder() << "CreateVolume returned " << StatusToString(response.operation().status());
            return false;
        }

        return true;
    }

    bool DropVolume(const TString& path, TString* error) {
        Ydb::KeyValue::DropVolumeRequest request;
        request.set_path(path);

        Ydb::KeyValue::DropVolumeResponse response;
        if (!CallWithRetry([&](grpc::ClientContext* ctx) { return Stub_->DropVolume(ctx, request, &response); }, error)) {
            return false;
        }

        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            *error = TStringBuilder() << "DropVolume returned " << StatusToString(response.operation().status());
            return false;
        }

        return true;
    }

    bool Write(const TString& path, ui32 partitionId, const TVector<std::pair<TString, TString>>& kvPairs, ui32 channel, TString* error) {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        for (const auto& [key, value] : kvPairs) {
            auto* write = request.add_commands()->mutable_write();
            write->set_key(key);
            write->set_value(value);
            write->set_storage_channel(channel);
        }

        Ydb::KeyValue::ExecuteTransactionResponse response;
        if (!CallWithRetry([&](grpc::ClientContext* ctx) { return Stub_->ExecuteTransaction(ctx, request, &response); }, error)) {
            return false;
        }

        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            *error = TStringBuilder() << "Write returned " << StatusToString(response.operation().status());
            return false;
        }

        return true;
    }

    bool DeleteKey(const TString& path, ui32 partitionId, const TString& key, TString* error) {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        auto* deleteRange = request.add_commands()->mutable_delete_range();
        deleteRange->mutable_range()->set_from_key_inclusive(key);
        deleteRange->mutable_range()->set_to_key_inclusive(key);

        Ydb::KeyValue::ExecuteTransactionResponse response;
        if (!CallWithRetry([&](grpc::ClientContext* ctx) { return Stub_->ExecuteTransaction(ctx, request, &response); }, error)) {
            return false;
        }

        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            *error = TStringBuilder() << "Delete returned " << StatusToString(response.operation().status());
            return false;
        }

        return true;
    }

    bool Read(const TString& path, ui32 partitionId, const TString& key, ui32 offset, ui32 size, TString* value, TString* error) {
        Ydb::KeyValue::ReadRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);
        request.set_key(key);
        request.set_offset(offset);
        request.set_size(size);

        Ydb::KeyValue::ReadResponse response;
        if (!CallWithRetry([&](grpc::ClientContext* ctx) { return Stub_->Read(ctx, request, &response); }, error)) {
            return false;
        }

        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            *error = TStringBuilder() << "Read returned " << StatusToString(response.operation().status());
            return false;
        }

        Ydb::KeyValue::ReadResult readResult;
        if (!response.operation().result().UnpackTo(&readResult)) {
            *error = "Read result unpack failed";
            return false;
        }

        *value = readResult.value();
        return true;
    }

private:
    template <typename TCall>
    bool CallWithRetry(TCall call, TString* error) {
        for (ui32 attempt = 1; attempt <= GrpcRetryCount; ++attempt) {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + GrpcDeadline);

            grpc::Status grpcStatus = call(&context);
            if (grpcStatus.ok()) {
                return true;
            }

            if (attempt == GrpcRetryCount) {
                *error = TStringBuilder()
                    << "gRPC call failed: code=" << static_cast<int>(grpcStatus.error_code())
                    << " message='" << grpcStatus.error_message() << "'";
                return false;
            }

            std::this_thread::sleep_for(GrpcRetrySleep);
        }

        *error = "gRPC call failed: unknown error";
        return false;
    }

private:
    std::shared_ptr<grpc::Channel> Channel_;
    std::unique_ptr<Ydb::KeyValue::V1::KeyValueService::Stub> Stub_;
};

class TWorker {
public:
    TWorker(
        ui32 workerId,
        const TOptions& options,
        const NKikimrKeyValue::KeyValueVolumeStressLoad& config,
        const TString& hostPort,
        const TString& volumePath,
        TRunStats& stats)
        : WorkerId_(workerId)
        , Options_(options)
        , Config_(config)
        , VolumePath_(volumePath)
        , Stats_(stats)
        , Client_(hostPort)
        , EndAt_(std::chrono::steady_clock::now() + std::chrono::seconds(options.Duration))
    {
        for (const auto& action : Config_.actions()) {
            ActionsByName_[action.name()] = &action;
            if (action.has_parent_action() && !action.parent_action().empty()) {
                ChildrenByParent_[action.parent_action()].push_back(action.name());
            }
        }

        if (static_cast<int>(Config_.partition_mode()) == 1 && Config_.volume_config().partition_count() > 0) {
            FixedPartitionId_ = WorkerId_ % Config_.volume_config().partition_count();
        }
    }

    void Run() {
        try {
            WriteInitialData();

            for (const auto& action : Config_.actions()) {
                if (action.has_parent_action() && !action.parent_action().empty()) {
                    continue;
                }

                if (action.has_period_us() && action.period_us() > 0) {
                    Schedulers_.emplace_back([this, name = action.name(), period = action.period_us()] {
                        PeriodicLoop(name, period);
                    });
                } else {
                    ScheduleAction(action.name());
                }
            }

            while (std::chrono::steady_clock::now() < EndAt_) {
                std::this_thread::sleep_for(100ms);
            }

            StopRequested_.store(true);

            for (auto& scheduler : Schedulers_) {
                scheduler.join();
            }

            WaitForActions();
        } catch (const std::exception& e) {
            Stats_.RecordError("worker_exception", e.what());
        }
    }

private:
    bool IsStopped() const {
        return StopRequested_.load(std::memory_order_relaxed);
    }

    void WaitForActions() {
        std::unique_lock lock(ActiveActionsMutex_);
        const bool completed = ActiveActionsCv_.wait_for(lock, 30s, [this] {
            return ActiveActions_.load(std::memory_order_relaxed) == 0;
        });

        if (!completed) {
            Stats_.RecordError("worker_shutdown_timeout", "waiting for active actions timed out");
        }
    }

    void PeriodicLoop(const TString& actionName, ui32 periodUs) {
        auto period = std::chrono::microseconds(std::max<ui32>(1, periodUs));
        auto nextRun = std::chrono::steady_clock::now();

        while (!IsStopped() && nextRun < EndAt_) {
            ScheduleAction(actionName);
            nextRun += period;
            std::this_thread::sleep_until(nextRun);
        }
    }

    void ScheduleAction(const TString& actionName) {
        if (IsStopped()) {
            return;
        }

        const auto actionIt = ActionsByName_.find(actionName);
        if (actionIt == ActionsByName_.end()) {
            Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
            return;
        }

        const auto* action = actionIt->second;
        const ui32 limit = action->has_worker_max_in_flight() && action->worker_max_in_flight() > 0
            ? action->worker_max_in_flight()
            : std::numeric_limits<ui32>::max();

        {
            std::lock_guard lock(RunningByActionMutex_);
            ui32& running = RunningByAction_[actionName];
            if (running >= limit) {
                return;
            }
            ++running;
        }

        ActiveActions_.fetch_add(1, std::memory_order_relaxed);

        std::thread([this, actionName] {
            ExecuteAction(actionName);

            {
                std::lock_guard lock(RunningByActionMutex_);
                auto it = RunningByAction_.find(actionName);
                if (it != RunningByAction_.end() && it->second) {
                    --it->second;
                }
            }

            if (ActiveActions_.fetch_sub(1, std::memory_order_relaxed) == 1) {
                std::lock_guard lock(ActiveActionsMutex_);
                ActiveActionsCv_.notify_all();
            }
        }).detach();
    }

    void ExecuteAction(const TString& actionName) {
        const auto actionIt = ActionsByName_.find(actionName);
        if (actionIt == ActionsByName_.end()) {
            Stats_.RecordError("unknown_action", TStringBuilder() << "Unknown action " << actionName);
            return;
        }

        const auto& action = *actionIt->second;

        for (const auto& command : action.action_command()) {
            switch (command.Command_case()) {
                case NKikimrKeyValue::ActionCommand::kPrint: {
                    Cerr << "[worker=" << WorkerId_ << "][action=" << actionName << "] " << command.print().msg() << Endl;
                    break;
                }
                case NKikimrKeyValue::ActionCommand::kRead: {
                    ExecuteReadCommand(action, command.read());
                    break;
                }
                case NKikimrKeyValue::ActionCommand::kWrite: {
                    ExecuteWriteCommand(actionName, command.write());
                    break;
                }
                case NKikimrKeyValue::ActionCommand::kDelete: {
                    ExecuteDeleteCommand(action, command.delete_());
                    break;
                }
                case NKikimrKeyValue::ActionCommand::COMMAND_NOT_SET: {
                    Stats_.RecordError("empty_command", TStringBuilder() << "Action " << actionName << " has empty command");
                    break;
                }
            }
        }

        Stats_.RecordAction(actionName);

        if (!IsStopped()) {
            const auto childrenIt = ChildrenByParent_.find(actionName);
            if (childrenIt != ChildrenByParent_.end()) {
                for (const TString& childName : childrenIt->second) {
                    ScheduleAction(childName);
                }
            }
        }
    }

    void WriteInitialData() {
        for (const auto& writeCommand : Config_.initial_data().write_commands()) {
            ExecuteWriteCommand("__initial__", writeCommand);
        }
    }

    void ExecuteWriteCommand(const TString& actionName, const NKikimrKeyValue::ActionCommand::Write& writeCommand) {
        if (writeCommand.count() == 0) {
            return;
        }

        const ui32 partitionId = SelectPartitionId();

        TVector<std::pair<TString, TString>> pairs;
        pairs.reserve(writeCommand.count());

        for (ui32 i = 0; i < writeCommand.count(); ++i) {
            const ui64 keyId = WriteKeyCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
            TString key = TStringBuilder() << actionName << "_" << WorkerId_ << "_" << keyId;
            TString value = GetPatternData(writeCommand.size());
            pairs.emplace_back(key, value);
        }

        TString error;
        if (!Client_.Write(VolumePath_, partitionId, pairs, writeCommand.channel(), &error)) {
            Stats_.RecordError("write_failed", error);
            return;
        }

        for (const auto& [key, value] : pairs) {
            DataStorage_.AddKey(actionName, key, TKeyInfo{partitionId, static_cast<ui32>(value.size())});
        }

        if (Options_.Verbose) {
            Cerr << "[worker=" << WorkerId_ << "] write ok action=" << actionName << " count=" << pairs.size() << Endl;
        }
    }

    void ExecuteReadCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand::Read& readCommand) {
        if (readCommand.count() == 0) {
            return;
        }

        const TVector<TString> sources = ResolveSources(action);
        const auto keys = DataStorage_.PickKeys(sources, readCommand.count(), false);

        for (const auto& [key, info] : keys) {
            const ui32 maxOffset = info.KeySize > readCommand.size() ? info.KeySize - readCommand.size() : 0;
            ui32 offset = 0;
            if (maxOffset > 0) {
                std::uniform_int_distribution<ui32> distribution(0, maxOffset);
                offset = distribution(RandomEngine());
            }

            TString value;
            TString error;
            if (!Client_.Read(VolumePath_, info.PartitionId, key, offset, readCommand.size(), &value, &error)) {
                Stats_.RecordError("read_failed", error);
                continue;
            }

            if (readCommand.verify_data()) {
                TString expected = GetPatternData(info.KeySize);
                expected = expected.substr(offset, readCommand.size());
                if (value != expected) {
                    Stats_.RecordError(
                        "verify_failed",
                        TStringBuilder()
                            << "key=" << key
                            << " partition=" << info.PartitionId
                            << " offset=" << offset
                            << " requested_size=" << readCommand.size()
                            << " expected_len=" << expected.size()
                            << " actual_len=" << value.size());
                }
            }
        }
    }

    void ExecuteDeleteCommand(const NKikimrKeyValue::Action& action, const NKikimrKeyValue::ActionCommand::Delete& deleteCommand) {
        if (deleteCommand.count() == 0) {
            return;
        }

        const TVector<TString> sources = ResolveSources(action);
        const auto keys = DataStorage_.PickKeys(sources, deleteCommand.count(), true);

        for (const auto& [key, info] : keys) {
            TString error;
            if (!Client_.DeleteKey(VolumePath_, info.PartitionId, key, &error)) {
                Stats_.RecordError("delete_failed", error);
            }
        }
    }

    TVector<TString> ResolveSources(const NKikimrKeyValue::Action& action) const {
        if (!action.has_action_data_mode()) {
            return {"__initial__"};
        }

        const auto& mode = action.action_data_mode();
        switch (mode.Mode_case()) {
            case NKikimrKeyValue::ActionDataMode::kWorker:
                return {"__initial__"};
            case NKikimrKeyValue::ActionDataMode::kFromPrevActions: {
                TVector<TString> names;
                for (const auto& name : mode.from_prev_actions().action_name()) {
                    names.push_back(name);
                }
                return names;
            }
            case NKikimrKeyValue::ActionDataMode::MODE_NOT_SET:
                return {"__initial__"};
        }

        return {"__initial__"};
    }

    ui32 SelectPartitionId() {
        if (FixedPartitionId_) {
            return *FixedPartitionId_;
        }

        const ui32 partitionCount = std::max<ui32>(1, Config_.volume_config().partition_count());
        std::uniform_int_distribution<ui32> distribution(0, partitionCount - 1);
        return distribution(RandomEngine());
    }

    TString GetPatternData(ui32 size) {
        std::lock_guard lock(PatternCacheMutex_);
        auto it = PatternCache_.find(size);
        if (it == PatternCache_.end()) {
            it = PatternCache_.emplace(size, GeneratePatternData(size)).first;
        }
        return it->second;
    }

private:
    const ui32 WorkerId_;
    const TOptions& Options_;
    const NKikimrKeyValue::KeyValueVolumeStressLoad& Config_;
    const TString VolumePath_;
    TRunStats& Stats_;
    TKeyValueClientV1 Client_;

    std::atomic<bool> StopRequested_ = false;
    const std::chrono::steady_clock::time_point EndAt_;

    TDataStorage DataStorage_;

    THashMap<TString, const NKikimrKeyValue::Action*> ActionsByName_;
    THashMap<TString, TVector<TString>> ChildrenByParent_;

    std::optional<ui32> FixedPartitionId_;

    std::mutex RunningByActionMutex_;
    THashMap<TString, ui32> RunningByAction_;

    std::atomic<ui64> ActiveActions_ = 0;
    std::mutex ActiveActionsMutex_;
    std::condition_variable ActiveActionsCv_;

    std::mutex PatternCacheMutex_;
    THashMap<ui32, TString> PatternCache_;

    std::atomic<ui64> WriteKeyCounter_ = 0;

    TVector<std::thread> Schedulers_;
};

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

    opts.AddLongOption("version", "KeyValue API version (currently only v1 is supported)")
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

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);
    Y_UNUSED(parseResult);

    if (options.InFlight == 0) {
        throw std::runtime_error("--in-flight must be > 0");
    }

    if (options.Duration == 0) {
        throw std::runtime_error("--duration must be > 0");
    }

    if (options.ConfigPath.empty() == options.ConfigName.empty()) {
        throw std::runtime_error("exactly one of --config or --config-name must be provided");
    }

    if (options.Version != "v1") {
        throw std::runtime_error("only --version v1 is supported in this cutover step");
    }

    return options;
}

int RunWorkload(const TOptions& options, const NKikimrKeyValue::KeyValueVolumeStressLoad& config) {
    const TString hostPort = ParseHostPort(options.Endpoint);
    const TString volumePath = MakeVolumePath(options.Database, config.volume_config().path());

    TRunStats stats;

    {
        TKeyValueClientV1 setupClient(hostPort);
        TString error;
        TVector<TString> channels;
        channels.reserve(config.volume_config().channel_media_size());
        for (const auto& media : config.volume_config().channel_media()) {
            channels.push_back(media);
        }

        if (!setupClient.CreateVolume(volumePath, config.volume_config().partition_count(), channels, &error)) {
            Cerr << "CreateVolume failed: " << error << Endl;
            return 2;
        }
    }

    {
        TVector<std::thread> threads;
        TVector<std::unique_ptr<TWorker>> workers;

        workers.reserve(options.InFlight);
        threads.reserve(options.InFlight);

        for (ui32 workerId = 0; workerId < options.InFlight; ++workerId) {
            workers.emplace_back(std::make_unique<TWorker>(workerId, options, config, hostPort, volumePath, stats));
            threads.emplace_back([worker = workers.back().get()] {
                worker->Run();
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }

    {
        TKeyValueClientV1 setupClient(hostPort);
        TString error;
        if (!setupClient.DropVolume(volumePath, &error)) {
            Cerr << "DropVolume failed: " << error << Endl;
            return 2;
        }
    }

    stats.PrintSummary();

    if (stats.GetTotalErrors() > 0 && !options.AllowErrors) {
        return 1;
    }

    return 0;
}

} // namespace

int main(int argc, char** argv) {
    try {
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
