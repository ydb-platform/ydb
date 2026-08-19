#pragma once

#include <ydb/core/protos/blobstorage_config.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr {

inline std::optional<TString> ValidateInferPDiskSlotCountSettingsForDriveType(
        const NKikimrBlobStorage::TInferPDiskSlotCountForDriveTypeSettings& settings,
        TStringBuf context) {
    const bool hasSlotSize = settings.HasSlotSize() && settings.GetSlotSize();
    const bool hasUnitSize = settings.HasUnitSize() && settings.GetUnitSize();
    const bool hasMaxSlots = settings.HasMaxSlots() && settings.GetMaxSlots();
    if (hasSlotSize && hasUnitSize) {
        return TStringBuilder() << context
            << " SlotSize is mutually exclusive with UnitSize"
            << " in InferPDiskSlotCountSettings";
    }

    if ((hasSlotSize || hasUnitSize) && !hasMaxSlots) {
        return TStringBuilder() << context
            << " MaxSlots is mandatory with SlotSize or UnitSize"
            << " in InferPDiskSlotCountSettings";
    }

    if (hasMaxSlots && !hasSlotSize && !hasUnitSize) {
        return TStringBuilder() << context
            << " MaxSlots requires SlotSize or UnitSize"
            << " in InferPDiskSlotCountSettings";
    }

    return {};
}

inline std::optional<TString> ValidateInferPDiskSlotCountSettings(
        const NKikimrBlobStorage::TInferPDiskSlotCountSettings& settings,
        TStringBuf context) {
    if (settings.HasRot()) {
        const TString rotContext = TStringBuilder() << context << ".Rot";
        if (auto error = ValidateInferPDiskSlotCountSettingsForDriveType(
                settings.GetRot(), rotContext)) {
            return error;
        }
    }

    if (settings.HasSsd()) {
        const TString ssdContext = TStringBuilder() << context << ".Ssd";
        if (auto error = ValidateInferPDiskSlotCountSettingsForDriveType(
                settings.GetSsd(), ssdContext)) {
            return error;
        }
    }

    return {};
}

} // NKikimr
