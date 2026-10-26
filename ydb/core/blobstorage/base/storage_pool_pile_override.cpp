#include "storage_pool_pile_override.h"
#include "pdisk_filter.h"

#include <ydb/core/base/blobstorage_grouptype.h>

#include <util/generic/hash_set.h>

namespace NKikimr {

const NKikimrBlobStorage::TStoragePoolPileOverride* FindStoragePoolPileOverride(
        const NKikimrConfig::TBlobStorageConfig& config, TStringBuf pileName, std::optional<TStringBuf> kind) {
    for (const auto& rule : config.GetStoragePoolPileOverrides()) {
        if (std::find(rule.GetPileName().begin(), rule.GetPileName().end(), pileName) != rule.GetPileName().end() && (kind
                ? rule.GetAllKinds() || std::find(rule.GetKind().begin(), rule.GetKind().end(), *kind) != rule.GetKind().end()
                : rule.GetStaticGroupOverride())) {
            return &rule;
        }
    }
    return nullptr;
}

std::optional<TString> ValidateStoragePoolPileOverrides(const NKikimrConfig::TBlobStorageConfig& config) {
    THashSet<TString> allKinds, dynamicPiles, staticPiles;
    THashSet<std::pair<TString, TString>> kinds;
    for (const auto& rule : config.GetStoragePoolPileOverrides()) {
        if (!rule.PileNameSize()) {
            return "storage pool pile override has no piles";
        }
        if (!rule.GetAllKinds() && !rule.KindSize() && !rule.GetStaticGroupOverride()) {
            return "storage pool pile override has no targets";
        }
        if (rule.GetAllKinds() && rule.KindSize()) {
            return "storage pool pile override specifies both AllKinds and Kind";
        }
        if (rule.HasErasureSpecies()) {
            TBlobStorageGroupType::EErasureSpecies erasure;
            if (!TBlobStorageGroupType::ParseErasureName(erasure, rule.GetErasureSpecies())) {
                return "invalid ErasureSpecies in storage pool pile override";
            }
            if (!rule.HasGeometry()) {
                return "storage pool pile override with ErasureSpecies requires Geometry (possibly empty)";
            }
        }
        if (!rule.HasErasureSpecies() && !rule.HasGeometry() && !rule.HasPDiskFilters()) {
            return "storage pool pile override has no settings";
        }
        THashSet<TString> rulePiles;
        for (const auto& pile : rule.GetPileName()) {
            if (!rulePiles.insert(pile).second) {
                return "duplicate pile name in storage pool pile override";
            }
            if (rule.GetStaticGroupOverride() && !staticPiles.insert(pile).second) {
                return "overlapping static group pile overrides";
            }
            if (rule.GetAllKinds()) {
                if (dynamicPiles.contains(pile) || !allKinds.insert(pile).second) {
                    return "overlapping storage pool pile overrides";
                }
            } else if (rule.KindSize()) {
                if (allKinds.contains(pile)) {
                    return "overlapping storage pool pile overrides";
                }
                for (const auto& kind : rule.GetKind()) {
                    if (!kinds.emplace(pile, kind).second) {
                        return "overlapping storage pool pile overrides";
                    }
                }
                dynamicPiles.insert(pile);
            }
        }
        for (const auto& filter : rule.GetPDiskFilters().GetPDiskFilter()) {
            THashSet<int> properties;
            for (const auto& property : filter.GetProperty()) {
                if (property.GetPropertyCase() == NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::PROPERTY_NOT_SET
                        || !properties.insert(property.GetPropertyCase()).second) {
                    return "missing or duplicate property in storage pool pile override PDiskFilter";
                }
                if (property.HasLocation() && !IsValidPDiskFilterLocation(property.GetLocation())) {
                    return "invalid Location in storage pool pile override PDiskFilter";
                }
            }
        }
    }
    return std::nullopt;
}

template<typename T>
void ApplyOverride(T& config, const NKikimrBlobStorage::TStoragePoolPileOverride& rule) {
    if (rule.HasErasureSpecies()) {
        config.SetErasureSpecies(rule.GetErasureSpecies());
    }
    if (rule.HasGeometry()) {
        config.MutableGeometry()->CopyFrom(rule.GetGeometry());
    }
    if (rule.HasPDiskFilters()) {
        config.MutablePDiskFilter()->CopyFrom(rule.GetPDiskFilters().GetPDiskFilter());
    }
}

void ApplyStoragePoolPileOverride(NKikimrBlobStorage::TDefineStoragePool& pool,
        const NKikimrBlobStorage::TStoragePoolPileOverride& rule) {
    ApplyOverride(pool, rule);
}

void ApplyStoragePoolPileOverride(NKikimrConfig::TSelfManagementConfig& config,
        const NKikimrBlobStorage::TStoragePoolPileOverride& rule) {
    ApplyOverride(config, rule);
    if (rule.HasPDiskFilters()) {
        config.ClearPDiskType();
    }
}

} // namespace NKikimr
