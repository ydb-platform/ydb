#pragma once

namespace NKikimr::NOlap::NIndexes {

class IIndexHeader {
private:
    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const ui64 cat) const = 0;

public:
    std::optional<TBlobRange> GetAddressForCategory(const ui64 cat) const {
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
        auto result = GetAddressRange(address, blobRange);
        AFL_VERIFY(result);
        return *result;
    }
};

class TDefaultHeader: public IIndexHeader {
private:
    const ui32 Size;

    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const ui64 cat) const override {
        AFL_VERIFY(!cat)("cat", cat);
        return TBlobRangeLink16(0, Size);
    }

public:
    TDefaultHeader(const ui32 size)
        : Size(size) {
    }
};

}   // namespace NKikimr::NOlap::NIndexes
