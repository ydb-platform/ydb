#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreSchemaUpdate: public TInStoreTableUpdate {
private:
    using TBase = TInStoreTableUpdate;
    std::optional<TOlapTTLUpdate> AlterTTL;
    std::optional<NKikimrSchemeOp::TColumnTableSchemaPreset> SchemaPreset;
    std::shared_ptr<TInStoreTable> TargetInStoreTable;
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;

    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const override {
        return TargetInStoreTable;
    }

    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const override {
        return TargetInStoreTable->GetTableInfoPtrVerified();
    }

    virtual TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody result;
        result.MutableSeqNo()->SetGeneration(seqNo.Generation);
        result.MutableSeqNo()->SetRound(seqNo.Round);

        auto& alter = *result.MutableAlterTable();
        FillToShardTx(alter);
        alter.SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
        return result.SerializeAsString();
    }

    void FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter) const;

    virtual std::set<ui64> DoGetShardIds() const override {
        return TargetInStoreTable->GetTableInfoVerified().GetShardIdsSet();
    }

public:

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }
};

}