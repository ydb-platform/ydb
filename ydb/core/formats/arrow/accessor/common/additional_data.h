#pragma once

#include <ydb/library/formats/arrow/protos/accessor.pb.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>
#include <optional>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NArrow::NAccessor {

struct IAdditionalAccessorData {
    virtual ~IAdditionalAccessorData() = default;
    // The proto to persist alongside the column, or nullopt when there is nothing to store. Both
    // consumers (scalar TIndexColumnMeta and sub-columns TColumn) carry this same message.
    virtual std::optional<NKikimrArrowAccessorProto::TAdditionalAccessorData> SerializeToProto() const = 0;

    virtual NJson::TJsonValue DebugJson() const {
        return NJson::TJsonValue(NJson::JSON_MAP);
    }
};

// Default "no extra metadata" implementation: carries nothing and serializes nothing.
// Used by accessors whose blob is self-describing (for now everything except dictionary encoding).
struct TEmptyAdditionalData : IAdditionalAccessorData {
    std::optional<NKikimrArrowAccessorProto::TAdditionalAccessorData> SerializeToProto() const override {
        return std::nullopt;
    }
};

// Extra metadata for a DICTIONARY-encoded column: the split of its blob into the dictionary blob
// and the positions blob (see NDictionary), which is not recoverable from the blob itself.
struct TDictionaryAccessorData : IAdditionalAccessorData {
    ui32 DictionaryBlobSize = 0;
    ui32 PositionsBlobSize = 0;

    TDictionaryAccessorData() = default;
    TDictionaryAccessorData(ui32 dictionaryBlobSize, ui32 positionsBlobSize)
        : DictionaryBlobSize(dictionaryBlobSize)
        , PositionsBlobSize(positionsBlobSize) {
    }

    std::optional<NKikimrArrowAccessorProto::TAdditionalAccessorData> SerializeToProto() const override;
    NJson::TJsonValue DebugJson() const override;
};

// Reconstruct additional accessor data from its serialized proto (inverse of SerializeToProto).
// Returns the concrete type for a known oneof branch, or empty metadata otherwise. The abstract
// entry point consumers use so they need not depend on the concrete accessor libraries.
std::shared_ptr<IAdditionalAccessorData> BuildAdditionalAccessorData(const NKikimrArrowAccessorProto::TAdditionalAccessorData& proto);

struct TBlobWithAdditionalAccessorData {
    TString Blob;
    std::shared_ptr<IAdditionalAccessorData> Meta;
};

}
