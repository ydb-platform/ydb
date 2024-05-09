#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

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

class TUpdateInitializationContext {
private:
    const TOperationContext* SSOperationContext = nullptr;
    const NKikimrSchemeOp::TModifyScheme* Modification = nullptr;
public:
    const TOperationContext* GetSSOperationContext() const {
        return SSOperationContext;
    }
    const NKikimrSchemeOp::TModifyScheme* GetModification() const {
        return Modification;
    }
    TUpdateInitializationContext(const TOperationContext* ssOperationContext, const NKikimrSchemeOp::TModifyScheme* modification)
        : SSOperationContext(ssOperationContext)
        , Modification(modification) {
        AFL_VERIFY(SSOperationContext);
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

class TEvolutionStartContext {
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

    TEvolutionStartContext(const TPath* objectPath, TOperationContext* ssOperationContext, NIceDb::TNiceDb* db)
        : ObjectPath(objectPath)
        , SSOperationContext(ssOperationContext)
        , DB(db) {
        AFL_VERIFY(DB);
        AFL_VERIFY(ObjectPath);
        AFL_VERIFY(SSOperationContext);
    }
};

}