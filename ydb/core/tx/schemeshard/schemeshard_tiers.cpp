#include "schemeshard_info_types.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/localdb.h>

#include <contrib/libs/zstd/include/zstd.h>

namespace NKikimr::NSchemeShard {

using namespace NKikimrSchemeOp;

void ApplyTierSettingsToFamily(const TTierDescription& tier, TFamilyDescription& family) {
    if (tier.HasCodec()) {
        family.SetColumnCodec(tier.GetCodec());
        if (tier.GetCodec() != ColumnCodecZSTD) {
            family.ClearColumnCodecLevel();
        }
    }
    if (tier.HasCodecLevel()) {
        family.SetColumnCodecLevel(tier.GetCodecLevel());
    }
    if (tier.HasCacheMode()) {
        family.SetColumnCacheMode(tier.GetCacheMode());
    }
    if (tier.HasStorageConfig()) {
        auto* storage = family.MutableStorageConfig();
        storage->MutableData()->MergeFrom(tier.GetStorageConfig());
        if (family.GetId() != 0) {
            storage->MutableExternal()->MergeFrom(tier.GetStorageConfig());
        }
    }
    if (tier.HasKeyIndexInDescriptor()) {
        family.SetKeyIndexInDescriptor(tier.GetKeyIndexInDescriptor());
    }
    if (tier.HasGroupIndexesInDescriptor()) {
        family.SetGroupIndexesInDescriptor(tier.GetGroupIndexesInDescriptor());
    }
}

bool ValidateColumnCodecLevel(EColumnCodec codec, i32 level, TString& errDescr) {
    if (codec != ColumnCodecZSTD) {
        errDescr = "COMPRESSION_LEVEL is supported only for ZSTD compression";
        return false;
    }
    const int maxLevel = ZSTD_maxCLevel();
    if (level < 0 || level > maxLevel) {
        errDescr = TStringBuilder() << "COMPRESSION_LEVEL for ZSTD must be in range [0, " << maxLevel << "]";
        return false;
    }
    return true;
}

namespace {

bool ApplyDefaultTier(TPartitionConfig& result, const TPartitionConfig& changes,
        const TTierDescription& tier, TString& errDescr) {
    if (tier.HasId() && tier.GetId() != 0) {
        errDescr = "The default tier must have id 0";
        return false;
    }
    for (const auto& family : changes.GetColumnFamilies()) {
        const bool isDefault = family.HasId() ? family.GetId() == 0
            : family.GetName().empty() || family.GetName() == "default";
        if (!isDefault) {
            continue;
        }
        // Both spellings address family 0. Applying their explicit settings in
        // either order must give the same result.
        auto combined = family;
        ApplyTierSettingsToFamily(tier, combined);
        TFamilyDescription reversed;
        ApplyTierSettingsToFamily(tier, reversed);
        reversed.MergeFrom(family);
        if (!google::protobuf::util::MessageDifferencer::Equals(combined, reversed)) {
            errDescr = "Conflicting settings for the default tier and the default column family";
            return false;
        }
    }
    TColumnFamiliesMerger families(result);
    auto* family = families.AddOrGet(0, errDescr);
    if (!family) {
        return false;
    }
    ApplyTierSettingsToFamily(tier, *family);
    return !family->HasColumnCodecLevel()
        || ValidateColumnCodecLevel(family->GetColumnCodec(), family->GetColumnCodecLevel(), errDescr);
}

} // namespace

bool TPartitionConfigMerger::ApplyChangesInTiers(TPartitionConfig& result, const TPartitionConfig& changes,
        const TAppData* appData, bool isServerlessDomain, TString& errDescr) {
    if (!changes.TiersSize() && !changes.AlterTiersSize()) {
        return true;
    }
    if (!appData->FeatureFlags.GetEnableDataTiering()) {
        errDescr = "Data tiering is not enabled";
        return false;
    }

    THashMap<TString, size_t> byName;
    THashSet<ui32> usedIds;
    ui32 lastId = 0;
    for (const auto& family : result.GetColumnFamilies()) {
        usedIds.insert(family.GetId());
        lastId = Max(lastId, family.GetId());
    }
    for (size_t index = 0; index < result.TiersSize(); ++index) {
        const auto& tier = result.GetTiers(index);
        byName.emplace(tier.GetName(), index);
        usedIds.insert(tier.GetId());
        lastId = Max(lastId, tier.GetId());
    }
    // Reserve explicit IDs before allocating IDs for name-only definitions.
    for (const auto& tier : changes.GetTiers()) {
        if (tier.HasId() && tier.GetName() != "default") {
            const ui32 id = tier.GetId();
            if (!id || id == Max<ui32>() || !usedIds.insert(id).second) {
                errDescr = TStringBuilder() << "Invalid or already used tier id " << id << " for tier " << tier.GetName();
                return false;
            }
        }
    }

    THashSet<TString> changedNames;
    for (bool add : {true, false}) {
        for (const auto& change : add ? changes.GetTiers() : changes.GetAlterTiers()) {
            const auto& name = change.GetName();
            if (name.empty()) {
                errDescr = "Missing tier name";
                return false;
            }
            if (!changedNames.insert(name).second) {
                errDescr = TStringBuilder() << "Multiple changes for tier " << name;
                return false;
            }
            if ((change.GetCodec() == ColumnCodecZSTD || change.HasCodecLevel()) &&
                    !appData->FeatureFlags.GetEnableRowTableZstdCompression()) {
                errDescr = "ZSTD compression for row tables is disabled";
                return false;
            }
            if (change.HasCacheMode() && !appData->FeatureFlags.GetEnableTableCacheModes()) {
                errDescr = "Setting cache_mode is not allowed";
                return false;
            }
            if (isServerlessDomain && change.GetCacheMode() == ColumnCacheModeTryKeepInMemory) {
                errDescr = TStringBuilder() << "CacheMode InMemory is not supported in serverless databases. Tier name: " << name;
                return false;
            }
            if (name == "default") {
                if (!ApplyDefaultTier(result, changes, change, errDescr)) {
                    return false;
                }
                continue;
            }

            const auto* index = byName.FindPtr(name);
            TTierDescription* tier;
            if (add) {
                if (index) {
                    errDescr = TStringBuilder() << "Tier already exists: " << name;
                    return false;
                }
                ui32 id = change.GetId();
                if (!change.HasId()) {
                    do {
                        if (lastId >= Max<ui32>() - 1) {
                            errDescr = TStringBuilder() << "Tier id overflow at adding tier " << name;
                            return false;
                        }
                        id = ++lastId;
                    } while (!usedIds.insert(id).second);
                }
                tier = result.AddTiers();
                tier->SetId(id);
            } else {
                if (!index) {
                    errDescr = TStringBuilder() << "Unknown tier: " << name;
                    return false;
                }
                tier = result.MutableTiers(*index);
                if (change.HasId() && change.GetId() != tier->GetId()) {
                    errDescr = TStringBuilder() << "Cannot change id of tier " << name;
                    return false;
                }
            }
            if (change.HasCodec() && change.GetCodec() != ColumnCodecZSTD) {
                tier->ClearCodecLevel();
            }
            tier->MergeFrom(change);
            if (tier->HasCodecLevel() && !ValidateColumnCodecLevel(tier->GetCodec(), tier->GetCodecLevel(), errDescr)) {
                return false;
            }
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
