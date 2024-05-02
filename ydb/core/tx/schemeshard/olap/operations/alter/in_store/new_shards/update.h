#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreReshardingUpdate: public TInStoreCommonUpdate {
private:
    NKikimrTxColumnShard::TCreateTable CreateToShard;
    std::set<ui64> NewShardIds;
    using TBase = TInStoreCommonUpdate;
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    virtual TConclusion<TEvolutions> DoBuildEvolutions() const override;
    virtual TVector<ISubOperation::TPtr> DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const override;
public:
    TInStoreReshardingUpdate(const std::shared_ptr<ISSEntity>& originalEntity, const NKikimrSchemeOp::TModifyScheme& request)
        : TBase(originalEntity, request) {
    }
};

}