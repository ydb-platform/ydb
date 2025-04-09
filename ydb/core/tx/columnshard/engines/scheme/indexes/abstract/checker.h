#pragma once
#include "common.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>
#include <util/digest/numeric.h>

namespace NKikimr::NOlap::NIndexes {

class TIndexDataAddress {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY_DEF(std::optional<ui64>, Category);

public:
    TString DebugString() const {
        if (Category) {
            return TStringBuilder() << "[" << IndexId << "," << *Category << "]";
        } else {
            return TStringBuilder() << "[" << IndexId << "]";
        }
    }

    template <class TContainer>
    static std::set<ui32> ExtractIndexIds(const TContainer& addresses) {
        std::set<ui32> result;
        for (auto&& i : addresses) {
            result.emplace(i.GetIndexId());
        }
        return result;
    }

    TIndexDataAddress() = default;

    explicit TIndexDataAddress(const ui32 indexId)
        : IndexId(indexId) {
        AFL_VERIFY(IndexId);
    }

    explicit TIndexDataAddress(const ui32 indexId, const std::optional<ui64> category)
        : IndexId(indexId)
        , Category(category) {
        AFL_VERIFY(IndexId);
    }

    bool operator<(const TIndexDataAddress& item) const {
        return std::tie(IndexId, Category) < std::tie(item.IndexId, item.Category);
    }

    bool operator==(const TIndexDataAddress& item) const {
        return std::tie(IndexId, Category) == std::tie(item.IndexId, item.Category);
    }

    operator size_t() const {
        if (Category) {
            return CombineHashes<ui64>(IndexId, *Category);
        } else {
            return IndexId;
        }
    }
};

}   // namespace NKikimr::NOlap::NIndexes
