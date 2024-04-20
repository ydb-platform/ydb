#pragma once
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <ydb/library/accessor/accessor.h>

#include <util/system/types.h>

#include <set>

namespace NKikimr::NSchemeShard {

template <class TSetElement>
class TLayoutIdSet {
private:
    std::set<TSetElement> Elements;
public:
    TLayoutIdSet() = default;
    TLayoutIdSet(const TSetElement elem) {
        Elements.emplace(elem);
    }

    typename std::set<TSetElement>::const_iterator begin() const {
        return Elements.begin();
    }

    typename std::set<TSetElement>::const_iterator end() const {
        return Elements.end();
    }

    size_t Size() const {
        return Elements.size();
    }

    std::vector<TSetElement> GetIdsVector() const {
        return std::vector<TSetElement>(Elements.begin(), Elements.end());
    }

    const std::set<TSetElement>& GetIds() const {
        return Elements;
    }

    std::set<TSetElement> GetIds(const ui32 count) const {
        std::set<TSetElement> result;
        ui32 idx = 0;
        for (auto&& i : Elements) {
            if (++idx > count) {
                return result;
            }
            result.emplace(i);
        }
        return result;
    }

    std::vector<TSetElement> GetIdsVector(const ui32 count) const {
        std::set<TSetElement> result = GetIds(count);
        return std::vector<TSetElement>(result.begin(), result.end());
    }

    bool AddId(const TSetElement& id) {
        return Elements.emplace(id).second;
    }

    bool RemoveId(const TSetElement& id) {
        return Elements.erase(id);
    }

    bool operator<(const TLayoutIdSet& item) const {
        if (Elements.size() < item.Elements.size()) {
            return true;
        }
        if (Elements.size() > item.Elements.size()) {
            return false;
        }
        auto itSelf = Elements.begin();
        auto itItem = item.Elements.begin();
        while (itSelf != Elements.end() && itItem != item.Elements.end()) {
            if (*itSelf < *itItem) {
                return true;
            } else if (*itSelf > *itItem) {
                return false;
            }
            ++itSelf;
            ++itItem;
        }
        if (itSelf != Elements.end() && itItem == item.Elements.end()) {
            return false;
        }
        if (itSelf == Elements.end() && itItem != item.Elements.end()) {
            return true;
        }
        return false;
    }
    bool operator==(const TLayoutIdSet& item) const {
        if (Elements.size() != item.Elements.size()) {
            return false;
        }
        auto itSelf = Elements.begin();
        auto itItem = item.Elements.begin();
        while (itSelf != Elements.end() && itItem != item.Elements.end()) {
            if (*itSelf != *itItem) {
                return false;
            }
            ++itSelf;
            ++itItem;
        }
        return true;
    }
};

class TSchemeShard;

class TColumnTablesLayout {
public:
    using TShardIdsGroup = TLayoutIdSet<ui64>;
    using TTableIdsGroup = TLayoutIdSet<TPathId>;

    class TTablesGroup {
    private:
        const TTableIdsGroup* TableIds = nullptr;
        YDB_READONLY_DEF(TShardIdsGroup, ShardIds);
    public:
        TTablesGroup() = default;
        TTablesGroup(const TTableIdsGroup* tableIds, TShardIdsGroup&& shardIds);

        const TTableIdsGroup& GetTableIds() const;

        bool TryMerge(const TTablesGroup& item);

        bool operator<(const TTablesGroup& item) const {
            return GetTableIds() < item.GetTableIds();
        }
    };

private:
    YDB_READONLY_DEF(std::vector<TTablesGroup>, Groups);
public:
    TColumnTablesLayout(std::vector<TTablesGroup>&& groups);

    static std::vector<ui64> ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss);

    static TColumnTablesLayout BuildTrivial(const std::vector<ui64>& tabletIds);
};

}
