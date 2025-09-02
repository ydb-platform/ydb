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
    THashSet<TSetElement> Elements;
public:
    TLayoutIdSet() = default;
    TLayoutIdSet(const TSetElement elem) {
        AddId(elem);
    }

    size_t Size() const {
        return Elements.size();
    }

    bool HasId(const TSetElement& id) const {
        return Elements.contains(id);
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
            return item.GetTableIds().HasId(pathId);
        };
        Groups.erase(std::remove_if(Groups.begin(), Groups.end(), pred), Groups.end());
    }

    static std::vector<ui64> ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss);

    static TColumnTablesLayout BuildTrivial(const std::vector<ui64>& tabletIds);
};

}
