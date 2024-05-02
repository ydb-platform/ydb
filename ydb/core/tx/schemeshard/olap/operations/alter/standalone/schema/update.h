#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/standalone/object.h>
#include <ydb/core/tx/schemeshard/olap/schema/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneSchemaUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
    std::set<ui64> ShardIds;
    NKikimrTxColumnShard::TAlterTable AlterToShard;
    std::shared_ptr<TStandaloneTable> OriginalStandalone;
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    virtual TConclusion<TEvolutions> DoBuildEvolutions() const override;
    virtual TVector<ISubOperation::TPtr> DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const override;

public:
    TStandaloneSchemaUpdate(const std::shared_ptr<ISSEntity>& original, const NKikimrSchemeOp::TModifyScheme& request)
        : TBase(original, request)
        , OriginalStandalone(dynamic_pointer_cast<TStandaloneTable>(GetOriginalEntity()))
    {
        AFL_VERIFY(!!OriginalStandalone);
    }
};

}