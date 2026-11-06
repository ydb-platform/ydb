#pragma once

#include <ydb/apps/bridge_skipper/domain/types.h>

#include <memory>

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

private:
    TStateFile(int directoryFd, int lockFd, const TString& clusterId);

    int DirectoryFd;
    int LockFd;
    TPersistentState State;
};

} // namespace NBridgeSkipper
