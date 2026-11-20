#include "state_file.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <cerrno>
#include <cstdio>
#include <exception>
#include <initializer_list>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace NBridgeSkipper {
namespace {

constexpr size_t MaxStateBytes = 1024 * 1024;
// The directory lock guarantees a single writer, so a fixed name suffices.
// A temporary left by a crash is truncated and reused by the next write.
constexpr char TemporaryName[] = "state.json.tmp";

class TFd {
public:
    explicit TFd(int value)
        : Value(value) {
    }

    TFd(const TFd&) = delete;
    TFd& operator=(const TFd&) = delete;

    ~TFd() {
        if (Value >= 0) {
            close(Value);
        }
    }

    int Get() const {
        return Value;
    }

    int Release() {
        return std::exchange(Value, -1);
    }

    bool Close() {
        return close(Release()) == 0;
    }

private:
    int Value;
};

void Require(bool condition, TStringBuf message) {
    Y_ENSURE(condition, message);
}

void Fields(const NJson::TJsonValue& value, std::initializer_list<TStringBuf> names) {
    Require(value.IsMap() && value.GetMap().size() == names.size(), "invalid state.json fields");
    for (const auto name : names) {
        Require(value.Has(name), "missing state.json field");
    }
}

TString String(const NJson::TJsonValue& value) {
    Require(value.IsString(), "state.json string field required");
    return value.GetString();
}

ui64 Generation(const NJson::TJsonValue& value) {
    const auto text = String(value);
    Require(!text.empty(), "invalid state.json generation");
    for (const char c : text) {
        Require(c >= '0' && c <= '9', "invalid state.json generation");
    }
    ui64 generation = 0;
    Require(TryFromString(text, generation), "invalid state.json generation");
    return generation;
}

EPileState ParseState(const NJson::TJsonValue& value) {
    const auto text = String(value);
    for (const auto state : {EPileState::Primary, EPileState::Promoted, EPileState::Synchronized,
            EPileState::NotSynchronized, EPileState::Suspended, EPileState::Disconnected, EPileState::Inactive}) {
        if (text == ToString(state)) {
            return state;
        }
    }
    throw yexception() << "invalid state.json pile state";
}

void ValidateState(const TPersistentState& state) {
    Require(!state.ClusterId.empty(), "invalid state.json cluster_id");
    if (const auto& pending = state.PendingMutation) {
        Require(!pending->Update.empty(), "invalid state.json empty update");
        for (const auto& [pile, value] : pending->Update) {
            (void)pile;
            Require(IsKnownState(value), "invalid state.json update");
        }
    }
}

// Only this process writes the file, so the default parser suffices; a file
// that does not match the schema fails closed.
TPersistentState Decode(TStringBuf bytes) {
    NJson::TJsonValue json;
    Require(NJson::ReadJsonTree(bytes, &json), "invalid state.json JSON");
    Require(json.Has("state_schema") && json["state_schema"].IsInteger() && json["state_schema"].GetInteger() == 3,
        "unsupported state.json state_schema: expected 3; unversioned state is not supported");
    Fields(json, {"state_schema", "mode", "cluster_id", "generation_high_water", "pending_mutation"});
    TPersistentState state;
    state.ClusterId = String(json["cluster_id"]);
    const auto mode = String(json["mode"]);
    Require(mode == "observe" || mode == "manual" || mode == "auto", "invalid state.json mode");
    state.Mode = mode == "auto" ? EMode::Auto : mode == "manual" ? EMode::Manual : EMode::Observe;
    state.GenerationHighWater = Generation(json["generation_high_water"]);
    const auto& pending = json["pending_mutation"];
    if (!pending.IsNull()) {
        Fields(pending, {"expected_generation", "update", "kind", "origin"});
        TPendingMutation record;
        const auto kind = String(pending["kind"]);
        bool knownKind = false;
        for (const auto value : {EOperationKind::Failover, EOperationKind::Switchover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
            if (ToString(value) == kind) {
                record.Kind = value;
                knownKind = true;
            }
        }
        Require(knownKind, "invalid state.json operation kind");
        const auto origin = String(pending["origin"]);
        Require(origin == "manual" || origin == "auto", "invalid state.json origin");
        record.Origin = origin == "manual" ? EDispatchOrigin::Manual : EDispatchOrigin::Auto;
        Require(record.Kind == EOperationKind::Failover || record.Origin == EDispatchOrigin::Manual, "invalid Auto lifecycle pending record");
        record.ExpectedGeneration = Generation(pending["expected_generation"]);
        const auto& update = pending["update"];
        Require(update.IsMap(), "invalid state.json update");
        for (const auto& [pile, value] : update.GetMap()) {
            record.Update.emplace(pile, ParseState(value));
        }
        Require(record.ExpectedGeneration <= state.GenerationHighWater, "state.json pending generation exceeds generation_high_water");
        state.PendingMutation = std::move(record);
    }
    ValidateState(state);
    return state;
}

TString Encode(const TPersistentState& state) {
    ValidateState(state);
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["state_schema"] = 3;
    json["mode"] = ToString(state.Mode);
    json["cluster_id"] = state.ClusterId;
    json["generation_high_water"] = ::ToString(state.GenerationHighWater);
    json["pending_mutation"] = NJson::TJsonValue(NJson::JSON_NULL);
    if (const auto& pending = state.PendingMutation) {
        auto& record = json["pending_mutation"];
        record.SetType(NJson::JSON_MAP);
        record["kind"] = ToString(pending->Kind);
        record["origin"] = ToString(pending->Origin);
        record["expected_generation"] = ::ToString(pending->ExpectedGeneration);
        auto& update = record["update"];
        update.SetType(NJson::JSON_MAP);
        for (const auto& [pile, value] : pending->Update) {
            update[pile] = ToString(value);
        }
    }
    auto bytes = NJson::WriteJson(&json, false, true, true);
    Require(bytes.size() <= MaxStateBytes, "state.json exceeds size limit");
    return bytes;
}

TString ReadState(int fd) {
    TString bytes;
    char buffer[4096];
    for (;;) {
        const ssize_t count = read(fd, buffer, sizeof(buffer));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        Require(count >= 0, "cannot read state.json");
        if (count == 0) {
            return bytes;
        }
        Require(bytes.size() + count <= MaxStateBytes, "state.json exceeds size limit");
        bytes.append(buffer, count);
    }
}

bool Sync(int fd) {
    int result;
    do {
        result = fsync(fd);
    } while (result < 0 && errno == EINTR);
    return result == 0;
}

} // namespace

TStateFile::TStateFile(int directoryFd, int lockFd, const TString& clusterId)
    : DirectoryFd(directoryFd)
    , LockFd(lockFd) {
    State.ClusterId = clusterId;
}

TStateFile::~TStateFile() {
    // Closing the descriptor releases flock, including on startup failure.
    close(LockFd);
    close(DirectoryFd);
}

std::unique_ptr<TStateFile> TStateFile::Open(const TString& directory, const TString& clusterId) {
    Require(!directory.empty(), "invalid state directory");
    Require(!clusterId.empty(), "invalid cluster_id");
    struct stat info;
    if (stat(directory.c_str(), &info) != 0) {
        Require(errno == ENOENT && mkdir(directory.c_str(), 0700) == 0, "cannot create state directory");
    }
    TFd dir(open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    Require(dir.Get() >= 0, "state directory must be a directory");
    TFd lock(openat(dir.Get(), "skipper.lock", O_RDWR | O_CREAT | O_NONBLOCK | O_CLOEXEC, 0600));
    Require(lock.Get() >= 0 && flock(lock.Get(), LOCK_EX | LOCK_NB) == 0, "state directory is locked or lock unavailable");
    auto result = std::unique_ptr<TStateFile>(new TStateFile(dir.Get(), lock.Get(), clusterId));
    dir.Release();
    lock.Release();
    TFd input(openat(result->DirectoryFd, "state.json", O_RDONLY | O_CLOEXEC));
    if (input.Get() < 0) {
        Require(errno == ENOENT, "cannot open state.json");
        Require(result->Write(result->State).empty(), "cannot create state.json");
    } else {
        auto state = Decode(ReadState(input.Get()));
        Require(state.ClusterId == clusterId, "state.json cluster_id does not match configuration");
        result->State = std::move(state);
    }
    return result;
}

TString TStateFile::Write(const TPersistentState& state) {
    LastWriteRenamed = false;
    TString bytes;
    try {
        Require(state.ClusterId == State.ClusterId, "state.json cluster_id does not match configuration");
        bytes = Encode(state);
    } catch (const std::exception&) {
        return "invalid persistent state";
    }
    TFd output(openat(DirectoryFd, TemporaryName, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600));
    if (output.Get() < 0) {
        return "cannot create state.json temporary file";
    }
    const auto fail = [&](TStringBuf error) {
        unlinkat(DirectoryFd, TemporaryName, 0);
        return TString(error);
    };
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t count = write(output.Get(), bytes.data() + offset, bytes.size() - offset);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            return fail("cannot write state.json temporary file");
        }
        offset += count;
    }
    if (!Sync(output.Get())) {
        return fail("cannot fsync state.json temporary file");
    }
    if (!output.Close()) {
        return fail("cannot close state.json temporary file");
    }
    if (renameat(DirectoryFd, TemporaryName, DirectoryFd, "state.json") != 0) {
        return fail("cannot rename state.json temporary file");
    }
    LastWriteRenamed = true;
    if ((DirectorySyncFailure && DirectorySyncFailure()) || !Sync(DirectoryFd)) {
        // The rename happened, so the file holds the new state while its
        // durability is uncertain. Memory follows the file: the actor keeps the
        // pending record it just wrote and stays blocked on the error until a
        // later write succeeds.
        State = state;
        return "cannot fsync state directory after rename; persistence uncertain";
    }
    State = state;
    return {};
}

TString TStateFile::ClearPending() {
    auto state = State;
    state.PendingMutation.reset();
    state.Mode = EMode::Observe;
    return Write(state);
}

} // namespace NBridgeSkipper
