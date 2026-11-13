#pragma once

#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <optional>
#include <utility>

namespace NKikimr::NViewer {

struct TBridgeGroupCounts {
    ui64 Required = 0;
    ui64 Synced = 0;
    ui64 Syncing = 0;
    ui64 Unknown = 0;

    void Add(const NKikimrBridge::TGroupSyncCounts& counts) {
        Required += counts.required();
        Synced += counts.synced();
        Syncing += counts.syncing();
        Unknown += counts.unknown();
    }

    void Add(const TBridgeGroupCounts& counts) {
        Required += counts.Required;
        Synced += counts.Synced;
        Syncing += counts.Syncing;
        Unknown += counts.Unknown;
    }

    NKikimrBridge::ESynchronizationStatus GetStatus() const {
        return GetBridgeSynchronizationStatus(Required, Synced, Syncing, Unknown);
    }
};

struct TBridgeDatabaseStats {
    NKikimrBridge::EStorageMode Mode = NKikimrBridge::STORAGE_MODE_UNSPECIFIED;
    TBridgeGroupCounts Groups;

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

class TBridgeStorageSnapshot {
    using TPoolId = std::pair<ui64, ui64>;

    struct TPoolStats {
        TString Name;
        NKikimrBridge::EStorageMode Mode = NKikimrBridge::STORAGE_MODE_UNSPECIFIED;
        TBridgeGroupCounts Groups;
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
                target.Groups.Add(row.groups());
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
