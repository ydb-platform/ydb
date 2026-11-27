#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NReplication::NController {

struct TGlobalWriteTxsConfig {
    ui64 MaxOpenWriteTxs = 0;
};

struct TOpenWriteTx {
    ui64 WriteTxId = 0;
    TRowVersion Version;
};

struct TWriteTxAssignment {
    TMap<TRowVersion, ui64> PendingVersionToWriteTxId;
    bool OpenWriteTxLimitReached = false;
    bool FreeWriteTxIdsExhausted = false;
};

struct TGlobalWriteTxsDiff {
    TMap<TRowVersion, ui64> Inserted;
    TVector<TRowVersion> Deleted;
};

class TGlobalWriteTxs {
public:
    explicit TGlobalWriteTxs(const TGlobalWriteTxsConfig& config);

    void RestoreWriteTxs(TMap<TRowVersion, ui64> versionToWriteTxId);

    void AddFreeWriteTxIds(const TVector<ui64>& writeTxIds);
    std::optional<ui64> TakeFreeWriteTxId();
    size_t CountFreeWriteTxIds() const;

    TWriteTxAssignment AssignWriteTxs(
        const TVector<TRowVersion>& pendingVersions,
        const TVector<TRowVersion>& boundaryVersions,
        bool reuseNextWriteTx);
    std::optional<TOpenWriteTx> AssignIndexBuildWriteTx(
        const TRowVersion& version,
        const TRowVersion& joinVersion,
        ui64 committingWriteTxId);
    ui64 OpenWriteTx(const TRowVersion& version);

    std::optional<TOpenWriteTx> FindNextWriteTxToCommit() const;
    void RemoveCommittedWriteTx(ui64 writeTxId);

    TVector<TOpenWriteTx> GetOpenWriteTxs() const;
    size_t CountOpenWriteTxs() const;
    bool HasOpenWriteTxs() const;
    bool HasUncommittedWriteTxs(const TVector<ui64>& writeTxIds) const;

    TGlobalWriteTxsDiff TakeChanges();
    void ClearOpenWriteTxs();

private:
    void MoveLastWriteTx(const TRowVersion& version);
    void InsertVersion(const TRowVersion& version, ui64 writeTxId);
    void EraseVersion(const TRowVersion& version);

    TGlobalWriteTxsConfig Config;
    TMap<TRowVersion, ui64> VersionToWriteTxId;
    TDeque<ui64> FreeWriteTxIds;
    TGlobalWriteTxsDiff Changes;
};

}
