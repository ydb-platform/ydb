#pragma once
#include "schemeshard_info_types.h"
#include <ydb/core/base/pathid.h>

namespace NKikimr::NSchemeShard {

class TTablesStorage {
private:
    THashMap<TPathId, TColumnTableInfo::TPtr> Tables;
    THashMap<TString, std::set<TPathId>> PathesByTieringId;

    void OnAddObject(const TPathId& pathId, TColumnTableInfo::TPtr object);
    void OnRemoveObject(const TPathId& pathId, TColumnTableInfo::TPtr object);
    TColumnTableInfo::TPtr ExtractPtr(const TPathId& id);
public:
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
            return Object.Get();
        }
        const TColumnTableInfo& operator*() const {
            return *Object;
        }
        const TColumnTableInfo* operator->() {
            return Object.Get();
        }
    };

    class TTableCreatedGuard: public TTableReadGuard, TMoveOnly {
    protected:
        const TPathId PathId;
        TTablesStorage& Owner;
    public:
        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id, TColumnTableInfo::TPtr object)
            : TTableReadGuard(object)
            , PathId(id)
            , Owner(owner)

        {
            Y_VERIFY(!Owner.contains(id));
        }

        TColumnTableInfo::TPtr GetPtr() const {
            return Object;
        }

        TTableCreatedGuard(TTablesStorage& owner, const TPathId& id)
            : TTableReadGuard(new TColumnTableInfo)
            , PathId(id)
            , Owner(owner) {
            Y_VERIFY(!Owner.contains(id));
        }
        TColumnTableInfo* operator->() {
            return Object.Get();
        }
        ~TTableCreatedGuard() {
            Y_VERIFY(Owner.Tables.emplace(PathId, Object).second);
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
