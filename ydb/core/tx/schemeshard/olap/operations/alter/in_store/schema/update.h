#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreSchemaUpdate: public TInStoreCommonUpdate {
private:
    using TBase = TInStoreCommonUpdate;
    NKikimrTxColumnShard::TAlterTable AlterToShard;
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus InitializeDiff(const NKikimrSchemeOp::TAlterColumnTable& alterCS);
    TConclusion<TOlapTTL> GetTtlPatched() const;
    virtual TVector<ISubOperation::TPtr> DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const override;
    virtual TConclusion<TEvolutions> DoBuildEvolutions() const override;
public:
    TInStoreSchemaUpdate(const std::shared_ptr<ISSEntity>& originalEntity, const NKikimrSchemeOp::TModifyScheme& request)
        : TBase(originalEntity, request) {
    }

    void FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter, const std::shared_ptr<TInStoreTable>& target) const;

};

}