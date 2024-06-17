#pragma once
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <ydb/library/accessor/accessor.h>

#include <util/digest/numeric.h>
#include <util/system/types.h>

#include <set>

namespace NKikimr::NSchemeShard {

template <class TSetElement, class THashCalcer>
class TLayoutIdSet {
private:
    ui64 Hash = 0;
    std::set<TSetElement> Elements;
public:
    TLayoutIdSet() = default;
    TLayoutIdSet(const TSetElement elem) {
        AddId(elem);
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
        bool result = Elements.emplace(id).second;
        if (result) {
            Hash ^= THashCalcer::GetHash(id);
        }
        return result;
    }

    bool RemoveId(const TSetElement& id) {
        auto result = Elements.erase(id);
        if (result) {
            Hash ^= THashCalcer::GetHash(id);
        }
        return result;
    }

    bool operator<(const TLayoutIdSet& item) const {
        if (Elements.size() < item.Elements.size()) {
            return true;
        }
        if (Elements.size() > item.Elements.size()) {
            return false;
        }
        return Hash < item.Hash;
    }
    bool operator==(const TLayoutIdSet& item) const {
        if (Elements.size() != item.Elements.size()) {
            return false;
        }
        return Hash == item.Hash;
    }
};

class TSchemeShard;

class TColumnTablesLayout {
private:
    class TPathIdHashCalcer {
    public:
        template <class T>
        static ui64 GetHash(const T& data) {
            return data.Hash();
        }
    };

public:
    using TTableIdsGroup = TLayoutIdSet<TPathId, TPathIdHashCalcer>;

    class TTablesGroup {
    private:
        const TTableIdsGroup* TableIds = nullptr;
        YDB_READONLY_DEF(std::set<ui64>, ShardIds);
    public:
        TTablesGroup() = default;
        TTablesGroup(const TTableIdsGroup* tableIds, std::set<ui64>&& shardIds);

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

    void RemoveGroupsWithPathId(const TPathId& pathId) {
        const auto pred = [&](const TTablesGroup& item) {
            return item.GetTableIds().GetIds().contains(pathId);
        };
        Groups.erase(std::remove_if(Groups.begin(), Groups.end(), pred), Groups.end());
    }

    static std::vector<ui64> ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss);

    static TColumnTablesLayout BuildTrivial(const std::vector<ui64>& tabletIds);
};

}
