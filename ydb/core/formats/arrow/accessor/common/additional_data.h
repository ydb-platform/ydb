#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>

namespace NKikimrTxColumnShard {
class TIndexColumnMeta;
}

namespace NKikimr::NArrow::NAccessor {

// Opaque accessor-specific metadata. Stored in portion index and passed through to the accessor at read/assembly.
// Callers that only pass data through do not need to know the concrete type.
// Each implementation adds its data to the chunk meta proto via AddToProto (only call when HasDataToSerialize()).
struct IAdditionalAccessorData {
    virtual ~IAdditionalAccessorData() = default;
    virtual bool HasDataToSerialize() const { return true; }
    virtual void AddToProto(NKikimrTxColumnShard::TIndexColumnMeta* meta) const = 0;
};

// Blob bytes plus optional accessor metadata. Used instead of std::pair<concrete_meta, TString> so
// callers can pass meta without knowing the concrete type (e.g. TDictionaryAccessorData).
struct TBlobWithAccessorMeta {
    TString Blob;
    std::shared_ptr<IAdditionalAccessorData> Meta;
};

}   // namespace NKikimr::NArrow::NAccessor
