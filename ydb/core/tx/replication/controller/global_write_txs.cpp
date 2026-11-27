#include "global_write_txs.h"

#include <util/generic/algorithm.h>
#include <util/system/yassert.h>

#include <utility>

namespace NKikimr::NReplication::NController {

TGlobalWriteTxs::TGlobalWriteTxs(const TGlobalWriteTxsConfig& config)
    : Config(config)
{
    Y_ABORT_UNLESS(Config.MaxOpenWriteTxs > 0);
}

void TGlobalWriteTxs::RestoreWriteTxs(TMap<TRowVersion, ui64> versionToWriteTxId) {
    VersionToWriteTxId = std::move(versionToWriteTxId);
    Changes = TGlobalWriteTxsDiff();
}

void TGlobalWriteTxs::AddFreeWriteTxIds(const TVector<ui64>& writeTxIds) {
    FreeWriteTxIds.insert(FreeWriteTxIds.end(), writeTxIds.begin(), writeTxIds.end());
}

std::optional<ui64> TGlobalWriteTxs::TakeFreeWriteTxId() {
    if (FreeWriteTxIds.empty()) {
        return std::nullopt;
    }

    const ui64 writeTxId = FreeWriteTxIds.front();
    FreeWriteTxIds.pop_front();
    return writeTxId;
}

size_t TGlobalWriteTxs::CountFreeWriteTxIds() const {
    return FreeWriteTxIds.size();
}

TWriteTxAssignment TGlobalWriteTxs::AssignWriteTxs(
    const TVector<TRowVersion>& pendingVersions,
    const TVector<TRowVersion>& boundaryVersions,
    bool reuseNextWriteTx)
{
    TWriteTxAssignment result;

    auto pending = pendingVersions.begin();
    for (; pending != pendingVersions.end(); ++pending) {
        const auto& version = *pending;
        const auto it = reuseNextWriteTx
            ? VersionToWriteTxId.lower_bound(version) : VersionToWriteTxId.find(version);
        if (it != VersionToWriteTxId.end()) {
            result.PendingVersionToWriteTxId.emplace(version, it->second);
            continue;
        }

        if (CountOpenWriteTxs() >= Config.MaxOpenWriteTxs) {
            result.OpenWriteTxLimitReached = true;
            break;
        }

        if (FreeWriteTxIds.empty()) {
            result.FreeWriteTxIdsExhausted = true;
            return result;
        }

        result.PendingVersionToWriteTxId.emplace(version, OpenWriteTx(version));
    }

    if (pending == pendingVersions.end()) {
        return result;
    }

    const auto& lastPendingVersion = pendingVersions.back();
    const auto previousVersion = VersionToWriteTxId.rbegin()->first;
    if (previousVersion < lastPendingVersion) {
        const bool crossesBoundary = AnyOf(boundaryVersions, [&](const TRowVersion& boundary) {
            return previousVersion <= boundary && boundary < lastPendingVersion;
        });
        if (crossesBoundary) {
            // C is a fixed boundary. At the open-transaction limit a
            // separate suffix is necessary instead of extending across C.
            if (FreeWriteTxIds.empty()) {
                return result;
            }

            OpenWriteTx(lastPendingVersion);
        } else {
            MoveLastWriteTx(lastPendingVersion);
        }
    }

    for (; pending != pendingVersions.end(); ++pending) {
        const auto it = VersionToWriteTxId.lower_bound(*pending);
        Y_ABORT_UNLESS(it != VersionToWriteTxId.end());
        result.PendingVersionToWriteTxId.emplace(*pending, it->second);
    }

    return result;
}

std::optional<TOpenWriteTx> TGlobalWriteTxs::AssignIndexBuildWriteTx(
    const TRowVersion& version,
    const TRowVersion& joinVersion,
    ui64 committingWriteTxId)
{
    if (const auto it = VersionToWriteTxId.lower_bound(version); it != VersionToWriteTxId.end()) {
        return TOpenWriteTx{.WriteTxId = it->second, .Version = it->first};
    }

    if (CountOpenWriteTxs() >= Config.MaxOpenWriteTxs) {
        const auto [lastVersion, lastWriteTxId] = *VersionToWriteTxId.rbegin();
        if (lastVersion > joinVersion && lastWriteTxId != committingWriteTxId) {
            MoveLastWriteTx(version);
            return TOpenWriteTx{.WriteTxId = lastWriteTxId, .Version = version};
        }
    }

    if (!FreeWriteTxIds.empty() && CountOpenWriteTxs() < Config.MaxOpenWriteTxs + 1) {
        return TOpenWriteTx{.WriteTxId = OpenWriteTx(version), .Version = version};
    }

    return std::nullopt;
}

ui64 TGlobalWriteTxs::OpenWriteTx(const TRowVersion& version) {
    const auto writeTxId = TakeFreeWriteTxId();
    Y_ABORT_UNLESS(writeTxId);
    InsertVersion(version, *writeTxId);
    return *writeTxId;
}

std::optional<TOpenWriteTx> TGlobalWriteTxs::FindNextWriteTxToCommit() const {
    if (VersionToWriteTxId.empty()) {
        return std::nullopt;
    }

    const auto& [version, writeTxId] = *VersionToWriteTxId.begin();
    return TOpenWriteTx{.WriteTxId = writeTxId, .Version = version};
}

void TGlobalWriteTxs::RemoveCommittedWriteTx(ui64 writeTxId) {
    const auto it = FindIf(VersionToWriteTxId, [writeTxId](const auto& item) {
        return item.second == writeTxId;
    });
    Y_ABORT_UNLESS(it != VersionToWriteTxId.end());
    const auto version = it->first;
    EraseVersion(version);
}

TVector<TOpenWriteTx> TGlobalWriteTxs::GetOpenWriteTxs() const {
    TVector<TOpenWriteTx> openWriteTxs;
    openWriteTxs.reserve(VersionToWriteTxId.size());
    for (const auto& [version, writeTxId] : VersionToWriteTxId) {
        openWriteTxs.push_back(TOpenWriteTx{.WriteTxId = writeTxId, .Version = version});
    }
    return openWriteTxs;
}

size_t TGlobalWriteTxs::CountOpenWriteTxs() const {
    return VersionToWriteTxId.size();
}

bool TGlobalWriteTxs::HasOpenWriteTxs() const {
    return !VersionToWriteTxId.empty();
}

bool TGlobalWriteTxs::HasUncommittedWriteTxs(const TVector<ui64>& writeTxIds) const {
    return AnyOf(VersionToWriteTxId, [&writeTxIds](const auto& item) {
        return ::Find(writeTxIds, item.second) != writeTxIds.end();
    });
}

TGlobalWriteTxsDiff TGlobalWriteTxs::TakeChanges() {
    return std::exchange(Changes, TGlobalWriteTxsDiff());
}

void TGlobalWriteTxs::ClearOpenWriteTxs() {
    VersionToWriteTxId.clear();
    Changes = TGlobalWriteTxsDiff();
}

void TGlobalWriteTxs::MoveLastWriteTx(const TRowVersion& version) {
    const auto [lastVersion, lastWriteTxId] = *VersionToWriteTxId.rbegin();
    EraseVersion(lastVersion);
    InsertVersion(version, lastWriteTxId);
}

void TGlobalWriteTxs::InsertVersion(const TRowVersion& version, ui64 writeTxId) {
    VersionToWriteTxId.emplace(version, writeTxId);
    Changes.Inserted[version] = writeTxId;
}

void TGlobalWriteTxs::EraseVersion(const TRowVersion& version) {
    VersionToWriteTxId.erase(version);
    Changes.Inserted.erase(version);
    if (::Find(Changes.Deleted, version) == Changes.Deleted.end()) {
        Changes.Deleted.push_back(version);
    }
}

}
