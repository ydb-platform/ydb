#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntity;

class TEntityInitializationContext {
private:
    const TOperationContext* SSOperationContext = nullptr;
public:
    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }
    TEntityInitializationContext(const TOperationContext* ssOperationContext)
        : SSOperationContext(ssOperationContext) {
        AFL_VERIFY(SSOperationContext);
    }
};

class TUpdateRestoreContext {
private:
    const ISSEntity* OriginalEntity;
    const TOperationContext* SSOperationContext = nullptr;
    const ui64 TxId;
public:
    ui64 GetTxId() const {
        return TxId;
    }

    const ISSEntity& GetOriginalEntity() const {
        return *OriginalEntity;
    }

    template <class T>
    const T& GetOriginalEntityAsVerified() const {
        auto* result = dynamic_cast<const T*>(OriginalEntity);
        AFL_VERIFY(!!result);
        return *result;
    }

    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }
    TUpdateRestoreContext(const ISSEntity* originalEntity, const TOperationContext* ssOperationContext, const ui64 txId)
        : OriginalEntity(originalEntity)
        , SSOperationContext(ssOperationContext)
        , TxId(txId) {
        AFL_VERIFY(OriginalEntity);
        AFL_VERIFY(SSOperationContext);
        AFL_VERIFY(TxId);
    }
};

class TUpdateInitializationContext: public TUpdateRestoreContext {
private:
    using TBase = TUpdateRestoreContext;
    const NKikimrSchemeOp::TModifyScheme* Modification = nullptr;
public:
    const NKikimrSchemeOp::TModifyScheme* GetModification() const {
        return Modification;
    }
    TUpdateInitializationContext(const ISSEntity* originalEntity, const TOperationContext* ssOperationContext, const NKikimrSchemeOp::TModifyScheme* modification, const ui64 txId)
        : TBase(originalEntity, ssOperationContext, txId)
        , Modification(modification) {
        AFL_VERIFY(Modification);
    }
};

class TEvolutionInitializationContext {
private:
    TOperationContext* SSOperationContext = nullptr;
public:
    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }
    TEvolutionInitializationContext(TOperationContext* ssOperationContext)
        : SSOperationContext(ssOperationContext) {
        AFL_VERIFY(SSOperationContext);
    }
};

class TUpdateStartContext {
private:
    const TPath* ObjectPath = nullptr;
    TOperationContext* SSOperationContext = nullptr;
    NIceDb::TNiceDb* DB;
public:
    const TPath* GetObjectPath() const {
        return ObjectPath;
    }
    NIceDb::TNiceDb* GetDB() const {
        return DB;
    }
    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }

    TUpdateStartContext(const TPath* objectPath, TOperationContext* ssOperationContext, NIceDb::TNiceDb* db)
        : ObjectPath(objectPath)
        , SSOperationContext(ssOperationContext)
        , DB(db)
    {
        AFL_VERIFY(DB);
        AFL_VERIFY(ObjectPath);
        AFL_VERIFY(SSOperationContext);
    }
};

class TUpdateFinishContext: public TUpdateStartContext {
private:
    using TBase = TUpdateStartContext;
    YDB_READONLY_DEF(std::optional<NKikimr::NOlap::TSnapshot>, Snapshot);
public:

    const NKikimr::NOlap::TSnapshot& GetSnapshotVerified() const {
        AFL_VERIFY(Snapshot);
        return *Snapshot;
    }

    TUpdateFinishContext(const TPath* objectPath, TOperationContext* ssOperationContext, NIceDb::TNiceDb* db, const std::optional<NKikimr::NOlap::TSnapshot>& ss)
        : TBase(objectPath, ssOperationContext, db)
        , Snapshot(ss)
    {
    }
};

}