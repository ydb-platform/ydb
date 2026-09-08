#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/olap/layout/layout.h>

namespace NKikimr::NSchemeShard {

struct TColumnTableInfo;

class TTablesStorage {
private:
    using TColumnTableInfoPtr = std::shared_ptr<TColumnTableInfo>;

    THashMap<TPathId, TColumnTableInfoPtr> Tables;
    THashMap<ui64, TColumnTablesLayout::TTableIdsGroup> TablesByShard;

    void OnAddObject(const TPathId& pathId, TColumnTableInfoPtr object);
    void OnRemoveObject(const TPathId& pathId, TColumnTableInfoPtr object);
    TColumnTableInfoPtr ExtractPtr(const TPathId& id);

public:
    std::unordered_set<TPathId> GetAllPathIds() const;
    TColumnTablesLayout GetTablesLayout(const std::vector<ui64>& tabletIds) const;
    const THashSet<TPathId>& GetTablesWithTier(const TString& storageId) const;

    class TTableReadGuard {
    protected:
        TColumnTableInfoPtr Object;

    public:
        bool operator!() const { return !Object; }

        TTableReadGuard(TColumnTableInfoPtr object)
            : Object(object)
        {}

        const TColumnTableInfo* operator->() const {
            Y_DEBUG_ABORT_UNLESS(Object);
            return Object.get();
        }

        const TColumnTableInfo& operator*() const {
            Y_DEBUG_ABORT_UNLESS(Object);
            return *Object;
        }

        TColumnTableInfoPtr GetPtr() const { return Object; }
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
        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id, TColumnTableInfoPtr object)
            : TTableCreateOperator(object)
            , PathId(id)
            , Owner(owner)
        {
            Y_ABORT_UNLESS(!Owner.contains(id));
        }

        TColumnTableInfoPtr GetPtr() const { return Object; }
        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id);

        TColumnTableInfo* operator->() { return Object.get(); }
        const TColumnTableInfo* operator->() const { return Object.get(); }

        ~TTableCreatedGuard() {
            Y_ABORT_UNLESS(Owner.Tables.emplace(PathId, Object).second);
            Owner.OnAddObject(PathId, Object);
        }
    };

    class TTableExtractedGuard: public TTableCreatedGuard {
    private:
        void UseAlterDataVerified();

    public:
        TTableExtractedGuard(TTablesStorage& owner, const TPathId& id, TColumnTableInfoPtr object, const bool extractAlter)
            : TTableCreatedGuard(owner, id, object)
        {
            Owner.OnRemoveObject(PathId, object);
            if (extractAlter) {
                UseAlterDataVerified();
            }
        }
    };

    TTableCreatedGuard BuildNew(const TPathId& id);
    TTableCreatedGuard BuildNew(const TPathId& id, TColumnTableInfoPtr object);
    TTableExtractedGuard TakeVerified(const TPathId& id);
    TTableExtractedGuard TakeAlterVerified(const TPathId& id);

    bool empty() const { return Tables.empty(); }
    bool contains(const TPathId& id) const { return Tables.contains(id); }

    TTableReadGuard GetVerified(const TPathId& id) const;
    TColumnTableInfoPtr GetVerifiedPtr(const TPathId& id) const;
    TTableReadGuard at(const TPathId& id) const { return TTableReadGuard(Tables.at(id)); }
    bool Drop(const TPathId& id);
};

} // namespace NKikimr::NSchemeShard
