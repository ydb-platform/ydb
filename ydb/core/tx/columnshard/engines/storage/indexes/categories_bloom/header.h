#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TCompositeBloomHeader: public IIndexHeader {
private:
    THashMap<ui64, TBlobRangeLink16> Ranges;
    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const ui64 cat) const override {
        auto it = Ranges.find(cat);
        if (it == Ranges.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

public:
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

    TCompositeBloomHeader(NKikimrTxColumnShard::TIndexCategoriesDescription&& proto, const ui32 headerShift) {
        ui32 currentOffset = headerShift;
        for (auto&& i : proto.GetCategory()) {
            for (auto&& h : i.GetHashes()) {
                Ranges.emplace(h, TBlobRangeLink16(currentOffset, currentOffset + i.GetFilterSize()));
            }
            currentOffset += i.GetFilterSize();
        }
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom
