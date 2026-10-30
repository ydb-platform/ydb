#pragma once

#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr {

// True when the storage sections define PDisk kinds or reference one by name; no allocations.
// Shared by distconf and the gRPC config service to refuse such configs for a BS_CONTROLLER that predates kinds.
inline bool UsesNamedPDiskKinds(const NKikimrConfig::TBlobStorageConfig& bsConfig,
        const NKikimrConfig::TSelfManagementConfig& smConfig) {
    if (bsConfig.PDiskKindsSize()) {
        return true;
    }
    for (const auto& hostConfig : bsConfig.GetDefineHostConfig()) {
        for (const auto& drive : hostConfig.GetDrive()) {
            if (!drive.GetKindName().empty()) {
                return true;
            }
        }
    }
    for (const auto& filter : smConfig.GetPDiskFilter()) {
        for (const auto& property : filter.GetProperty()) {
            if (property.HasKindName()) {
                return true;
            }
        }
    }
    return false;
}

// Effective settings of a drive that references a named PDisk kind: the kind profile with the drive's
// overrides applied on top. TPDiskConfig is proto2, so MergeFrom keeps explicit zero/false values and
// merges submessages field by field. BSC and the config validator must agree on this rule.
inline NKikimrBlobStorage::TPDiskConfig MergePDiskKindConfig(
        const NKikimrBlobStorage::TPDiskConfig& kind,
        const NKikimrBlobStorage::TPDiskConfig& overrides) {
    NKikimrBlobStorage::TPDiskConfig effective(kind);
    effective.MergeFrom(overrides);
    return effective;
}

inline std::optional<TString> ValidatePDiskConfig(
        const NKikimrBlobStorage::TPDiskConfig& config,
        TStringBuf context) {
    const bool hasExpectedSlotCount = config.HasExpectedSlotCount() && config.GetExpectedSlotCount();
    const bool hasSlotSizeInUnits = config.HasSlotSizeInUnits() && config.GetSlotSizeInUnits();
    const bool hasExpectedSlotSize = config.HasExpectedSlotSize() && config.GetExpectedSlotSize();
    const bool hasMaxSlots = config.HasMaxSlots() && config.GetMaxSlots();

    if (hasExpectedSlotSize && (hasExpectedSlotCount || hasSlotSizeInUnits)) {
        return TStringBuilder() << context
            << " ExpectedSlotSize is mutually exclusive with ExpectedSlotCount and SlotSizeInUnits"
            << " in PDiskConfig";
    }

    if (hasExpectedSlotSize && !hasMaxSlots) {
        return TStringBuilder() << context
            << " ExpectedSlotSize requires MaxSlots"
            << " in PDiskConfig";
    }

    if (hasMaxSlots && !hasExpectedSlotSize) {
        return TStringBuilder() << context
            << " MaxSlots requires ExpectedSlotSize"
            << " in PDiskConfig";
    }

    return {};
}

} // namespace NKikimr
