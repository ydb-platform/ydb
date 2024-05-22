#pragma once
#include "context.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ISSEntityEvolution {
private:
    YDB_READONLY_DEF(TPathId, PathId);
    bool Initialized = false;
protected:
    virtual TConclusionStatus DoInitialize(const TEvolutionInitializationContext& context) = 0;
    virtual TConclusionStatus DoStartEvolution(const TEvolutionStartContext& context) = 0;
    virtual NKikimrTxColumnShard::TSchemaTxBody DoGetShardTxBody(const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 tabletId) const = 0;
public:
    virtual ~ISSEntityEvolution() = default;
    TConclusionStatus Initialize(const TEvolutionInitializationContext& context) {
        AFL_VERIFY(!Initialized);
        Initialized = true;
        return DoInitialize(context);
    }

    TConclusionStatus StartEvolution(const TEvolutionStartContext& context) {
        return DoStartEvolution(context);
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const = 0;
    NKikimrTxColumnShard::TSchemaTxBody GetShardTxBody(const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 tabletId) const {
        return DoGetShardTxBody(original, tabletId);
    }

    ISSEntityEvolution(const TPathId& pathId)
        : PathId(pathId) {

    }
};

}