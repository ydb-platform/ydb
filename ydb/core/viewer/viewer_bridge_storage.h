#pragma once

#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <optional>
#include <utility>

namespace NKikimr::NViewer {

struct TBridgeGroupSyncStats {
    ui64 Required = 0;
    ui64 Synced = 0;
    ui64 Syncing = 0;
    ui64 Pending = 0;
    ui64 Unknown = 0;
    std::optional<ui64> BytesRemaining;
    bool BytesRemainingComplete = true;
    std::optional<ui64> BytesPerSecond = 0;

    TBridgeGroupSyncStats() = default;

    explicit TBridgeGroupSyncStats(const NKikimrBridge::TDatabaseSynchronization& row)
        : Required(row.groups().required())
        , Synced(row.groups().synced())
        , Syncing(row.groups().syncing())
        , Pending(row.groups().pending())
        , Unknown(row.groups().unknown())
        , BytesRemainingComplete(row.has_remaining_bytes() && row.remaining_bytes_complete())
    {
        // A completed pile contributes no estimate for copies still awaiting their first scan in another pile.
        if (Required != Synced && row.has_remaining_bytes()) {
            BytesRemaining = row.remaining_bytes();
        }
        if (row.has_copy_bytes_per_second()) {
            BytesPerSecond = row.copy_bytes_per_second();
        } else {
            BytesPerSecond.reset();
        }
    }

    void Add(const TBridgeGroupSyncStats& stats) {
        if (!stats.Required) {
            return;
        }
        Required += stats.Required;
        Synced += stats.Synced;
        Syncing += stats.Syncing;
        Pending += stats.Pending;
        Unknown += stats.Unknown;
        if (stats.BytesRemaining) {
            BytesRemaining = BytesRemaining.value_or(0) + *stats.BytesRemaining;
        }
        BytesRemainingComplete &= stats.BytesRemainingComplete;
        if (BytesPerSecond && stats.BytesPerSecond) {
            *BytesPerSecond += *stats.BytesPerSecond;
        } else {
            BytesPerSecond.reset();
        }
    }

    NKikimrBridge::ESynchronizationStatus GetStatus() const {
        return GetBridgeSynchronizationStatus(Required, Synced, Syncing, Unknown);
    }
};

struct TBridgeDatabaseStats {
    NKikimrBridge::EStorageMode Mode = NKikimrBridge::STORAGE_MODE_UNSPECIFIED;
    TBridgeGroupSyncStats Groups;

    NKikimrBridge::ESynchronizationStatus GetStatus() const {
        return Groups.GetStatus();
    }
};

inline std::optional<THashSet<TString>> GetBridgeDatabaseStoragePools(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& database,
        const NSchemeCache::TSchemeCacheNavigate::TEntry* resource = nullptr) {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    if (database.Status != TNavigate::EStatus::Ok || !database.DomainInfo || !database.DomainDescription) {
        return std::nullopt;
    }
    THashSet<TString> pools;
    const auto addPools = [&](const auto& description) {
        for (const auto& pool : description.GetStoragePools()) {
            pools.insert(pool.GetName());
        }
    };
    addPools(database.DomainDescription->Description);
    const auto& domain = *database.DomainInfo;
    if (domain.ResourcesDomainKey != domain.DomainKey) {
        if (!resource || resource->Status != TNavigate::EStatus::Ok || !resource->DomainInfo ||
                resource->DomainInfo->DomainKey != domain.ResourcesDomainKey || !resource->DomainDescription) {
            return std::nullopt;
        }
        addPools(resource->DomainDescription->Description);
    }
    return pools;
}

inline std::optional<THashMap<TString, const NKikimrSysView::TStoragePoolEntry*>> FindBridgeDatabasePoolEntries(
        const THashSet<TString>& names, const NKikimrSysView::TEvGetStoragePoolsResponse& pools) {
    THashMap<TString, const NKikimrSysView::TStoragePoolEntry*> entries;
    for (const auto& pool : pools.GetEntries()) {
        const auto& name = pool.GetInfo().GetName();
        if (names.contains(name) && !entries.emplace(name, &pool).second) {
            return std::nullopt;
        }
    }
    if (entries.size() != names.size()) {
        return std::nullopt;
    }
    return entries;
}

class TBridgeStorageSnapshot {
    using TPoolId = std::pair<ui64, ui64>;

    struct TPoolStats {
        TString Name;
        NKikimrBridge::EStorageMode Mode = NKikimrBridge::STORAGE_MODE_UNSPECIFIED;
        TBridgeGroupSyncStats Groups;
        THashSet<TString> PileNames;
        bool HasUnplaced = false;
        bool Valid = true;
    };

    bool Valid = false;
    THashMap<TPoolId, TPoolStats> Pools;
    THashMap<TString, std::optional<TPoolId>> PoolIdsByName;

public:
    explicit TBridgeStorageSnapshot(const NKikimrBridge::TStorageState& state) {
        if (state.GetStatus() != Ydb::StatusIds::SUCCESS || !state.HasDatabases()) {
            return;
        }
        Valid = true;
        for (const auto& row : state.GetDatabases().databases()) {
            for (const auto& pool : row.storage_pools()) {
                const TPoolId id(pool.id().box_id(), pool.id().pool_id());
                auto& target = Pools[id];
                const auto [named, inserted] = PoolIdsByName.emplace(pool.name(), id);
                if (!inserted && named->second && *named->second != id) {
                    named->second.reset();
                }
                if (row.storage_pools_size() != 1 || !id.second ||
                        (row.storage_mode() != NKikimrBridge::BRIDGED && row.storage_mode() != NKikimrBridge::ORDINARY) ||
                        !row.has_groups()) {
                    target.Valid = false;
                    continue;
                }
                if (target.Mode == NKikimrBridge::STORAGE_MODE_UNSPECIFIED) {
                    target.Name = pool.name();
                    target.Mode = row.storage_mode();
                } else if (target.Name != pool.name() || target.Mode != row.storage_mode()) {
                    target.Valid = false;
                }
                if (row.has_pile_name()) {
                    if (!target.PileNames.insert(row.pile_name()).second) {
                        target.Valid = false;
                        continue;
                    }
                } else if (target.HasUnplaced) {
                    target.Valid = false;
                    continue;
                } else {
                    target.HasUnplaced = true;
                }
                target.Groups.Add(TBridgeGroupSyncStats(row));
            }
        }
    }

    bool IsValid() const {
        return Valid;
    }

    std::optional<TBridgeDatabaseStats> Resolve(const THashSet<TString>& poolNames) const {
        if (!Valid || poolNames.empty()) {
            return std::nullopt;
        }
        TBridgeDatabaseStats result;
        for (const auto& name : poolNames) {
            const auto named = PoolIdsByName.find(name);
            if (named == PoolIdsByName.end() || !named->second) {
                return std::nullopt;
            }
            const auto& pool = Pools.at(*named->second);
            if (!pool.Valid || pool.Mode == NKikimrBridge::STORAGE_MODE_UNSPECIFIED) {
                return std::nullopt;
            }
            result.Mode = result.Mode == NKikimrBridge::STORAGE_MODE_UNSPECIFIED ? pool.Mode
                : result.Mode == pool.Mode ? pool.Mode : NKikimrBridge::MIXED;
            result.Groups.Add(pool.Groups);
        }
        return result;
    }
};

} // namespace NKikimr::NViewer
