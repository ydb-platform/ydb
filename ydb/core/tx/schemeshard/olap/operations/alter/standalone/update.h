#pragma once
#include "object.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/common/update.h>
#include <ydb/core/tx/schemeshard/olap/schema/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneSchemaUpdate: public TColumnTableUpdate {
private:
    using TBase = ISSEntityUpdate;
    std::optional<TOlapSchemaUpdate> AlterSchema;
    std::optional<TOlapTTLUpdate> AlterTTL;
    std::shared_ptr<TStandaloneTable> TargetStandalone;
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;

    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const override {
        return TargetStandalone->GetTableInfoPtrVerified();
    }

    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const override {
        return TargetStandalone;
    }

    virtual std::set<ui64> DoGetShardIds() const override {
        return TargetStandalone->GetTableInfoVerified().GetShardIdsSet();
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody result;
        result.MutableSeqNo()->SetGeneration(seqNo.Generation);
        result.MutableSeqNo()->SetRound(seqNo.Round);

        auto& alter = *result.MutableAlterTable();
        FillToShardTx(alter);
        alter.SetPathId(TargetStandalone->GetPathId().LocalPathId);
        return result.SerializeAsString();
    }

    void FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter) const {
        if (AlterSchema) {
            *shardAlter.MutableSchema() = TargetStandalone->GetTableSchemaProto();
        }
        if (AlterTTL) {
            *shardAlter.MutableTtlSettings() = TargetStandalone->GetTableTTLProto();
        }
    }
public:
};

}