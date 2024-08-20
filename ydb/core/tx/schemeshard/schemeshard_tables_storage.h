#pragma once
#include "schemeshard_info_types.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/accessor/accessor.h>
#include <util/digest/numeric.h>

namespace NKikimr::NSchemeShard {

template <class TSetElement>
class TLayoutIdSet {
private:
    std::set<TSetElement> Elements;
public:
    typename std::set<TSetElement>::const_iterator begin() const {
        return Elements.begin();
    }

    typename std::set<TSetElement>::const_iterator end() const {
        return Elements.end();
    }

    size_t Size() const {
        return Elements.size();
    }

    explicit operator ui64() const {
        return Hash();
    }

    ui64 Hash() const {
        ui64 result = 0;
        for (auto&& i : Elements) {
            result = CombineHashes(result, std::hash<TSetElement>()(i));
        }
        return result;
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
        YDB_READONLY_DEF(TTableIdsGroup, TableIds);
        YDB_READONLY_DEF(TShardIdsGroup, ShardIds);
    public:
        TTablesGroup(const TTableIdsGroup& tableIds, TShardIdsGroup&& shardIds)
            : TableIds(tableIds)
            , ShardIds(std::move(shardIds)) {

        }

        bool operator<(const TTablesGroup& item) const {
            return TableIds < item.TableIds;
        }
    };

private:
    YDB_READONLY_DEF(std::vector<TTablesGroup>, Groups);
public:
    TColumnTablesLayout(std::vector<TTablesGroup>&& groups)
        : Groups(std::move(groups)) {
        std::sort(Groups.begin(), Groups.end());
    }

    static std::vector<ui64> ShardIdxToTabletId(const std::vector<TShardIdx>& shards, const TSchemeShard& ss);

    static TColumnTablesLayout BuildTrivial(const std::vector<ui64>& tabletIds);
};

class TTablesStorage {
private:
    THashMap<TPathId, TColumnTableInfo::TPtr> Tables;
    THashMap<TString, std::set<TPathId>> PathsByTieringId;
    THashMap<ui64, TColumnTablesLayout::TTableIdsGroup> TablesByShard;

    void OnAddObject(const TPathId& pathId, TColumnTableInfo::TPtr object);
    void OnRemoveObject(const TPathId& pathId, TColumnTableInfo::TPtr object);
    TColumnTableInfo::TPtr ExtractPtr(const TPathId& id);
public:
    std::unordered_set<TPathId> GetAllPathIds() const;

    TColumnTablesLayout GetTablesLayout(const std::vector<ui64>& tabletIds) const;

    const std::set<TPathId>& GetTablesWithTiering(const TString& tieringId) const;

    class TTableReadGuard {
    protected:
        TColumnTableInfo::TPtr Object;
    public:
        bool operator!() const {
            return !Object;
        }

        TTableReadGuard(TColumnTableInfo::TPtr object)
            : Object(object) {
        }
        const TColumnTableInfo* operator->() const {
            Y_DEBUG_ABORT_UNLESS(Object);
            return Object.Get();
        }
        const TColumnTableInfo& operator*() const {
            Y_DEBUG_ABORT_UNLESS(Object);
            return *Object;
        }
        const TColumnTableInfo* operator->() {
            Y_DEBUG_ABORT_UNLESS(Object);
            return Object.Get();
        }
    };

    class TTableCreateOperator: public TTableReadGuard {
    private:
        using TBase = TTableReadGuard;
    public:
        using TBase::TBase;
        bool InitShardingTablets(const TColumnTablesLayout& currentLayout, const ui32 shardsCount, TOlapStoreInfo::ILayoutPolicy::TPtr layoutPolicy, bool& isNewGroup) const;
    };

    class TTableCreatedGuard: public TTableCreateOperator, TMoveOnly {
    protected:
        const TPathId PathId;
        TTablesStorage& Owner;
    public:
        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id, TColumnTableInfo::TPtr object)
            : TTableCreateOperator(object)
            , PathId(id)
            , Owner(owner)

        {
            Y_ABORT_UNLESS(!Owner.contains(id));
        }

        TColumnTableInfo::TPtr GetPtr() const {
            return Object;
        }

        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id)
            : TTableCreateOperator(new TColumnTableInfo)
            , PathId(id)
            , Owner(owner) {
            Y_ABORT_UNLESS(!Owner.contains(id));
        }
        TColumnTableInfo* operator->() {
            return Object.Get();
        }
        const TColumnTableInfo* operator->() const {
            return Object.Get();
        }
        ~TTableCreatedGuard() {
            Y_ABORT_UNLESS(Owner.Tables.emplace(PathId, Object).second);
            Owner.OnAddObject(PathId, Object);
        }
    };

    class TTableExtractedGuard: public TTableCreatedGuard {
    private:
        void UseAlterDataVerified();
    public:
        TTableExtractedGuard(TTablesStorage& owner, const TPathId& id, TColumnTableInfo::TPtr object, const bool extractAlter)
            : TTableCreatedGuard(owner, id, object)
        {
            Owner.OnRemoveObject(PathId, object);
            if (extractAlter) {
                UseAlterDataVerified();
            }
        }
    };

    TTableCreatedGuard BuildNew(const TPathId& id);
    TTableCreatedGuard BuildNew(const TPathId& id, TColumnTableInfo::TPtr object);
    TTableExtractedGuard TakeVerified(const TPathId& id);
    TTableExtractedGuard TakeAlterVerified(const TPathId& id);
    bool empty() const {
        return Tables.empty();
    }
    bool contains(const TPathId& id) const {
        return Tables.contains(id);
    }
    TTableReadGuard GetVerified(const TPathId& id) const;
    TTableReadGuard at(const TPathId& id) const {
        return TTableReadGuard(Tables.at(id));
    }
    size_t Drop(const TPathId& id);
};

}
