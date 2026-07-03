#pragma once

#include <ydb/core/protos/blobstorage_pdisk_config.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr {

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

    return {};
}

} // namespace NKikimr
