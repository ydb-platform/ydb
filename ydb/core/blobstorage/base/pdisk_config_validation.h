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
    const bool hasExpectedSlotSize = config.HasExpectedSlotSize() && config.GetExpectedSlotSize();
    const bool hasMaxSlots = config.HasMaxSlots() && config.GetMaxSlots();

    if (hasExpectedSlotSize && hasExpectedSlotCount) {
        return TStringBuilder() << context
            << " ExpectedSlotSize is mutually exclusive with ExpectedSlotCount"
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
