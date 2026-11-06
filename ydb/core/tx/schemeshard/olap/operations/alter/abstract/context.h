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
        YDB_LOG_VERIFY(SSOperationContext);
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
        YDB_LOG_VERIFY(!!result);
        return *result;
    }

    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }
    TUpdateRestoreContext(const ISSEntity* originalEntity, const TOperationContext* ssOperationContext, const ui64 txId)
        : OriginalEntity(originalEntity)
        , SSOperationContext(ssOperationContext)
        , TxId(txId) {
        YDB_LOG_VERIFY(OriginalEntity);
        YDB_LOG_VERIFY(SSOperationContext);
        YDB_LOG_VERIFY(TxId);
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
        YDB_LOG_VERIFY(Modification);
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
        YDB_LOG_VERIFY(SSOperationContext);
    }
};

class TUpdateStartContext {
private:
    const TPath* ObjectPath = nullptr;
    TProposeContext* SSOperationContext = nullptr;
public:
    const TPath* GetObjectPath() const {
        return ObjectPath;
    }
    const TProposeContext* GetSSOperationContext() const {
        return SSOperationContext;
    }

    TUpdateStartContext(const TPath* objectPath, TProposeContext* ssOperationContext)
        : ObjectPath(objectPath)
        , SSOperationContext(ssOperationContext)
    {
        YDB_LOG_VERIFY(ObjectPath);
        YDB_LOG_VERIFY(SSOperationContext);
    }
};

class TUpdateFinishContext {
private:
    const TPath* ObjectPath = nullptr;
    TOperationContext* SSOperationContext = nullptr;
    YDB_READONLY_DEF(std::optional<NKikimr::NOlap::TSnapshot>, Snapshot);
public:
    const TPath* GetObjectPath() const {
        return ObjectPath;
    }
    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }

    const NKikimr::NOlap::TSnapshot& GetSnapshotVerified() const {
        YDB_LOG_VERIFY(Snapshot);
        return *Snapshot;
    }

    TUpdateFinishContext(const TPath* objectPath, TOperationContext* ssOperationContext, const std::optional<NKikimr::NOlap::TSnapshot>& ss)
        : ObjectPath(objectPath)
        , SSOperationContext(ssOperationContext)
        , Snapshot(ss)
    {
        YDB_LOG_VERIFY(ObjectPath);
        YDB_LOG_VERIFY(SSOperationContext);
    }
};

}
