#pragma once
#include "abstract.h"

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneTableInfoConstructor: public ITableInfoConstructor {
protected:
    virtual bool DoInitialize(const TTablesStorage::TTableReadGuard& /*originalTableInfo*/, const TInitializationContext& /*iContext*/, IErrorCollector& /*errors*/) override {
        return true;
    }

    virtual NKikimrTxColumnShard::TSchemaTxBody DoGetShardTxBody(const TTablesStorage::TTableReadGuard& originalTableInfo, const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 /*tabletId*/) const override {
        NKikimrTxColumnShard::TSchemaTxBody copy = original;
        auto& alter = *copy.MutableAlterTable();
        auto& alterInfo = *originalTableInfo->AlterData;
        if (alterInfo.Description.HasSchema()) {
            *alter.MutableSchema() = alterInfo.Description.GetSchema();
        }
        AFL_VERIFY(!alterInfo.Description.HasSchemaPresetId());
        return copy;
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual bool DoCustomStartToSS(const TTablesStorage::TTableReadGuard& /*tableInfo*/, TStartSSContext& /*context*/, IErrorCollector& /*errors*/) override {
        return true;
    }

    virtual TConclusion<bool> DoPatchSchema(TOlapSchema& originalSchema) const override {
        TSimpleErrorCollector collector;
        if (!AlterRequest.HasAlterSchema()) {
            return false;
        }
        TOlapSchemaUpdate schemaUpdate;
        if (!schemaUpdate.Parse(AlterRequest.GetAlterSchema(), collector)) {
            return TConclusionStatus::Fail(collector->GetErrorMessage());
        }

        if (!originalSchema.Update(schemaUpdate, collector)) {
            return TConclusionStatus::Fail(collector->GetErrorMessage());
        }
        return true;
    }

    virtual bool DoInitializeFromProto(IErrorCollector& /*errors*/) override {
        return true;
    }

    virtual const NKikimrSchemeOp::TColumnTableSchema* GetTableSchema(const TTablesStorage::TTableExtractedGuard& tableInfo, IErrorCollector& errors) const override {
        if (!tableInfo->Description.HasSchema()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "No schema for standalone column table");
            return nullptr;
        }
        return &tableInfo->Description.GetSchema();
    }
};


}