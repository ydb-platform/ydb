#pragma once

#include <ydb/apps/bridge_skipper/domain/types.h>

#include <memory>
#include <functional>

namespace NBridgeSkipper {

// Owns the nonblocking directory lock for its lifetime. Startup errors throw.
class TStateFile {
public:
    static std::unique_ptr<TStateFile> Open(const TString& directory, const TString& clusterId);
    ~TStateFile();

    TStateFile(const TStateFile&) = delete;
    TStateFile& operator=(const TStateFile&) = delete;

    const TPersistentState& GetState() const {
        return State;
    }

    // Empty means durable success. On any error the caller must block mutation
    // dispatch. Memory retains the last acknowledged state, except after a
    // failed directory fsync following the rename, where it follows the renamed
    // file; a later full Write re-establishes durability.
    TString Write(const TPersistentState& state);
    TString ClearPending();

    // Describes the most recent Write even when the requested values already
    // matched the old file. A rename can still leave durability uncertain.
    bool WasLastWriteRenamed() const {
        return LastWriteRenamed;
    }

    // Deterministic durability seam for tests. No production command exposes it.
    void SetDirectorySyncFailureForTests(std::function<bool()> fail) {
        DirectorySyncFailure = std::move(fail);
    }

private:
    TStateFile(int directoryFd, int lockFd, const TString& clusterId);

    int DirectoryFd;
    int LockFd;
    TPersistentState State;
    bool LastWriteRenamed = false;
    std::function<bool()> DirectorySyncFailure;
};

} // namespace NBridgeSkipper
