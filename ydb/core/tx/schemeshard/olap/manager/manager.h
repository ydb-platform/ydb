#pragma once
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/olap/layout/layout.h>
#include <ydb/core/tx/schemeshard/olap/store/store.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>

namespace NKikimr::NSchemeShard {

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
            return Object.get();
        }
        const TColumnTableInfo& operator*() const {
            Y_DEBUG_ABORT_UNLESS(Object);
            return *Object;
        }
    };

    class TTableCreateOperator: public TTableReadGuard {
    private:
        using TBase = TTableReadGuard;
    public:
        using TBase::TBase;
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
            : TTableCreateOperator(std::make_shared<TColumnTableInfo>())
            , PathId(id)
            , Owner(owner) {
            Y_ABORT_UNLESS(!Owner.contains(id));
        }
        TColumnTableInfo* operator->() {
            return Object.get();
        }
        const TColumnTableInfo* operator->() const {
            return Object.get();
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
    TColumnTableInfo::TPtr GetVerifiedPtr(const TPathId& id) const;
    TTableReadGuard at(const TPathId& id) const {
        return TTableReadGuard(Tables.at(id));
    }
    size_t Drop(const TPathId& id);
};

}
