#pragma once
#include "checker.h"

#include <ydb/core/tx/columnshard/common/blob.h>

namespace NKikimr::NOlap::NIndexes {

class IIndexHeader {
private:
    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const std::optional<ui64> cat) const = 0;

public:
    virtual ~IIndexHeader() = default;

    static TConclusion<ui32> ReadHeaderSize(const TString& data, const bool withSizeBytes) {
        if (data.size() < 4) {
            return TConclusionStatus::Fail("too small data size");
        } else {
            TStringInput si(data);
            ui32 result;
            si.Read(&result, sizeof(result));
            if (withSizeBytes) {
                return 4 + result;
            } else {
                return result;
            }
        }
    }

    static TConclusion<TStringBuf> ReadHeader(const TString& data) {
        auto size = ReadHeaderSize(data, false);
        if (size.IsFail()) {
            return size;
        } else if (data.size() + 4 < *size) {
            return TConclusionStatus::Fail("too small data (from header read)");
        } else {
            return TStringBuf(data.data() + 4, *size);
        }
    }

    std::optional<TBlobRangeLink16> GetAddressForCategory(const std::optional<ui64> cat) const {
        return DoGetAddressForCategory(cat);
    }

    std::optional<TBlobRange> GetAddressRangeOptional(const TIndexDataAddress& address, const TBlobRange& blobRange) const {
        auto range = GetAddressForCategory(address.GetCategory());
        if (!range) {
            return std::nullopt;
        } else {
            return blobRange.BuildSubset(range->GetOffset(), range->GetSize());
        }
    }

    TBlobRange GetAddressRangeVerified(const TIndexDataAddress& address, const TBlobRange& blobRange) const {
        auto result = GetAddressRangeOptional(address, blobRange);
        AFL_VERIFY(result);
        return *result;
    }
};

class TDefaultHeader: public IIndexHeader {
private:
    const ui32 Size;

    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const std::optional<ui64> cat) const override {
        AFL_VERIFY(!cat)("cat", cat);
        return TBlobRangeLink16(0, Size);
    }

public:
    TDefaultHeader(const ui32 size)
        : Size(size) {
    }
};

}   // namespace NKikimr::NOlap::NIndexes
