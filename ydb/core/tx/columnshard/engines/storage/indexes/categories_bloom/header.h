#pragma once
#include <ydb/core/tx/columnshard/engines/protos/index.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TCompositeBloomHeader: public IIndexHeader {
private:
    THashMap<ui64, TBlobRangeLink16> Ranges;
    virtual std::optional<TBlobRangeLink16> DoGetAddressForCategory(const std::optional<ui64> cat) const override {
        AFL_VERIFY(!!cat);
        auto it = Ranges.find(*cat);
        if (it == Ranges.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

public:
    TCompositeBloomHeader(NKikimrTxColumnShard::TIndexCategoriesDescription&& proto, const ui32 headerShift) {
        ui32 currentOffset = headerShift;
        for (auto&& i : proto.GetCategories()) {
            for (auto&& h : i.GetHashes()) {
                Ranges.emplace(h, TBlobRangeLink16(currentOffset, currentOffset + i.GetFilterSize()));
            }
            currentOffset += i.GetFilterSize();
        }
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom
