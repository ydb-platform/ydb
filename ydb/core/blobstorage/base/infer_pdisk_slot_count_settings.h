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
    const bool hasLegacySettings = settings.HasUnitSize() || settings.HasMaxSlots()
        || settings.HasPreferInferredSettingsOverExplicit();
    if (hasSlotSize && hasLegacySettings) {
        return TStringBuilder() << context
            << " SlotSize is mutually exclusive with UnitSize, MaxSlots and PreferInferredSettingsOverExplicit"
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
