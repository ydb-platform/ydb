#pragma once

#include <util/system/types.h>

#include <memory>

namespace NKikimr::NArrow::NAccessor {

// Visitor for accessor-specific metadata. Used when serializing to proto or when a consumer needs a concrete type.
struct IAdditionalAccessorDataVisitor {
    virtual ~IAdditionalAccessorDataVisitor() = default;
    virtual void VisitDictionary(ui32 variantsBlobSize, ui32 recordsBlobSize) = 0;
};

// Opaque accessor-specific metadata. Stored in portion index and passed through to the accessor at read/assembly.
// Callers that only pass data through do not need to know the concrete type.
struct IAdditionalAccessorData {
    virtual ~IAdditionalAccessorData() = default;
    virtual void Accept(IAdditionalAccessorDataVisitor& visitor) const = 0;
};

// Dictionary accessor metadata (Variants+Records blob layout).
struct TDictionaryAccessorData : IAdditionalAccessorData {
    ui32 VariantsBlobSize = 0;
    ui32 RecordsBlobSize = 0;

    TDictionaryAccessorData() = default;
    TDictionaryAccessorData(ui32 variantsBlobSize, ui32 recordsBlobSize)
        : VariantsBlobSize(variantsBlobSize)
        , RecordsBlobSize(recordsBlobSize) {
    }

    void Accept(IAdditionalAccessorDataVisitor& visitor) const override {
        visitor.VisitDictionary(VariantsBlobSize, RecordsBlobSize);
    }
};

}   // namespace NKikimr::NArrow::NAccessor
