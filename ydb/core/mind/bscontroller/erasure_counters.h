#pragma once

#include <ydb/core/erasure/erasure.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/string/printf.h>

#include <map>

namespace NKikimr::NBsController {

// BSC-owned metadata. Values are dimensionless; usage counters keep their raw byte units.
class TStorageErasureCounters {
    using TEntry = std::pair<TString, ui32>;
    NMonitoring::TDynamicCounterPtr Parent;
    NMonitoring::TDynamicCounterPtr Root;
    std::map<TString, TEntry> Groups;
    std::map<TString, TEntry> Pools;

    void Set(std::map<TString, TEntry>& entries, const TString& key, const TString& id,
            const TString& pool, ui32 species, const TString& sensor) {
        if (pool.empty() || species >= TErasureType::ErasureSpeciesCount) {
            Erase(entries, key, id);
            return;
        }
        const TEntry entry(pool, species);
        if (const auto it = entries.find(id); it != entries.end() && it->second == entry) {
            return;
        }
        auto value = MakeIntrusive<NMonitoring::TDynamicCounters>();
        *value->GetSubgroup("storagePool", pool)
            ->GetSubgroup("erasureSpecies", TErasureType::ErasureSpeciesName(species))
            ->GetCounter(sensor) = 1;
        // Readers observe either the old or the new complete mapping, never both.
        Root->GetSubgroup(key, id);
        Root->ReplaceSubgroup(key, id, value);
        entries[id] = entry;
    }

    void Erase(std::map<TString, TEntry>& entries, const TString& key, const TString& id) {
        Root->RemoveSubgroup(key, id);
        entries.erase(id);
    }

public:
    explicit TStorageErasureCounters(NMonitoring::TDynamicCounterPtr parent)
        : Parent(std::move(parent))
        , Root(MakeIntrusive<NMonitoring::TDynamicCounters>())
    {
        Parent->GetSubgroup("subsystem", "erasureMapping");
        Parent->ReplaceSubgroup("subsystem", "erasureMapping", Root);
    }

    ~TStorageErasureCounters() {
        Parent->RemoveSubgroup("subsystem", "erasureMapping");
    }

    static TString GroupLabel(ui32 groupId) {
        // Same formatting as TSkeletonFront::CreateVDiskCounters (DskUsedBytes).
        return Sprintf("%09" PRIu32, groupId);
    }

    void SetGroup(ui32 groupId, const TString& pool, ui32 species) {
        Set(Groups, "group", GroupLabel(groupId), pool, species, "GroupErasureInfo");
    }

    void EraseGroup(ui32 groupId) {
        Erase(Groups, "group", GroupLabel(groupId));
    }

    void SetPool(const TString& id, const TString& pool, ui32 species) {
        Set(Pools, "storagePoolId", id, pool, species, "StoragePoolErasureInfo");
    }

    void ErasePool(const TString& id) {
        Erase(Pools, "storagePoolId", id);
    }
};

} // namespace NKikimr::NBsController
