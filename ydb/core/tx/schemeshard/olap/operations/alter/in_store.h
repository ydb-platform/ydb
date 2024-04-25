#pragma once
#include "abstract.h"

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreTableInfoConstructor: public ITableInfoConstructor {
private:
    TOlapStoreInfo::TPtr StoreInfo;
protected:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual NKikimrTxColumnShard::TSchemaTxBody DoGetShardTxBody(const TTablesStorage::TTableReadGuard& originalTableInfo, const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 /*tabletId*/) const override {
        NKikimrTxColumnShard::TSchemaTxBody copy = original;
        auto& alter = *copy.MutableAlterTable();
        auto& alterInfo = *originalTableInfo->AlterData;
        AFL_VERIFY(!alterInfo.Description.HasSchema());
        if (alterInfo.Description.HasSchemaPresetId()) {
            const ui32 presetId = alterInfo.Description.GetSchemaPresetId();
            Y_ABORT_UNLESS(StoreInfo->SchemaPresets.contains(presetId), "Failed to find schema preset %" PRIu32 " in an olap store", presetId);
            auto& preset = StoreInfo->SchemaPresets.at(presetId);
            size_t presetIndex = preset.GetProtoIndex();
            *alter.MutableSchemaPreset() = StoreInfo->GetDescription().GetSchemaPresets(presetIndex);
        }
        return copy;
    }

    virtual bool DoInitialize(const TTablesStorage::TTableReadGuard& originalTableInfo, const TInitializationContext& iContext, IErrorCollector& errors) override;

    virtual bool DoCustomStartToSS(const TTablesStorage::TTableReadGuard& tableInfo, TStartSSContext& context, IErrorCollector& /*errors*/) override;

    virtual TConclusion<bool> DoPatchSchema(TOlapSchema& /*schema*/) const override {
        return false;
    }

    virtual bool DoInitializeFromProto(IErrorCollector& errors) override {
        if (AlterRequest.HasAlterSchema()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "Can't modify schema for table in store");
            return false;
        }
        return true;
    }

    const NKikimrSchemeOp::TColumnTableSchema* GetTableSchema(const TTablesStorage::TTableExtractedGuard& tableInfo, IErrorCollector& errors) const override;

private:
};

}