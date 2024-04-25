#pragma once
#include <ydb/core/tx/schemeshard/schemeshard__operation_part.h>
#include <ydb/core/tx/schemeshard/schemeshard_path.h>
#include <ydb/core/tx/schemeshard/olap/manager/manager.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class ITableInfoConstructor {
public:
    class TInitializationContext {
    private:
        TOperationContext* SSOperationContext = nullptr;
    public:
        const TOperationContext* GetSSOperationContext() const {
            return SSOperationContext;
        }
        TInitializationContext(TOperationContext* ssOperationContext)
            : SSOperationContext(ssOperationContext) {
            AFL_VERIFY(SSOperationContext);
        }
    };

    class TStartSSContext {
    private:
        const TPath* ObjectPath = nullptr;
        TOperationContext* SSOperationContext = nullptr;
        NIceDb::TNiceDb* DB;
    public:
        const TPath* GetObjectPath() const {
            return ObjectPath;
        }
        NIceDb::TNiceDb* GetDB() {
            return DB;
        }
        const TOperationContext* GetSSOperationContext() const {
            return SSOperationContext;
        }

        TStartSSContext(const TPath* objectPath, TOperationContext* ssOperationContext, NIceDb::TNiceDb* db)
            : ObjectPath(objectPath)
            , SSOperationContext(ssOperationContext)
            , DB(db) {
            AFL_VERIFY(DB);
            AFL_VERIFY(ObjectPath);
            AFL_VERIFY(SSOperationContext);
        }
    };

    virtual ~ITableInfoConstructor() = default;

protected:
    NKikimrSchemeOp::TAlterColumnTable AlterRequest;
    virtual bool DoInitializeFromProto(IErrorCollector& errors) = 0;
    virtual const NKikimrSchemeOp::TColumnTableSchema* GetTableSchema(const TTablesStorage::TTableExtractedGuard& tableInfo, IErrorCollector& errors) const = 0;
    virtual TConclusion<bool> DoPatchSchema(TOlapSchema& schema) const = 0;
    
    virtual bool DoPatchTTL(const TOlapSchema& patchedSchema, TColumnTableInfo& result, IErrorCollector& errors) const {
        const ui64 currentTtlVersion = result.Description.HasTtlSettings() ? result.Description.GetTtlSettings().GetVersion() : 0;
        if (AlterRequest.HasAlterTtlSettings()) {
            const auto& ttlUpdate = AlterRequest.GetAlterTtlSettings();
            if (ttlUpdate.HasUseTiering()) {
                result.Description.MutableTtlSettings()->SetUseTiering(ttlUpdate.GetUseTiering());
            }
            if (ttlUpdate.HasEnabled()) {
                if (!patchedSchema.ValidateTtlSettings(ttlUpdate, errors)) {
                    return false;
                }
                *result.Description.MutableTtlSettings()->MutableEnabled() = ttlUpdate.GetEnabled();
            }
            if (ttlUpdate.HasDisabled()) {
                *result.Description.MutableTtlSettings()->MutableDisabled() = ttlUpdate.GetDisabled();
            }
            result.Description.MutableTtlSettings()->SetVersion(currentTtlVersion + 1);
        }

        if (!patchedSchema.ValidateTtlSettings(result.Description.GetTtlSettings(), errors)) {
            return false;
        }
        return true;
    }

    TConclusion<bool> PatchSchema(TOlapSchema& schema) const {
        return DoPatchSchema(schema);
    }

    bool PatchTTL(const TOlapSchema& schema, TColumnTableInfo& result, IErrorCollector& errors) const {
        return DoPatchTTL(schema, result, errors);
    }

    virtual bool DoCustomStartToSS(const TTablesStorage::TTableReadGuard& tableInfo, TStartSSContext& context, IErrorCollector& errors) = 0;

    virtual bool DoInitialize(const TTablesStorage::TTableReadGuard& originalTableInfo, const TInitializationContext& iContext, IErrorCollector& errors) = 0;
    virtual NKikimrTxColumnShard::TSchemaTxBody DoGetShardTxBody(const TTablesStorage::TTableReadGuard& originalTableInfo, const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 tabletId) const = 0;
public:
    static std::shared_ptr<ITableInfoConstructor> Construct(const TTablesStorage::TTableReadGuard& tableInfo, const TInitializationContext& iContext, IErrorCollector& errors);

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const = 0;
    NKikimrTxColumnShard::TSchemaTxBody GetShardTxBody(const TTablesStorage::TTableReadGuard& originalTableInfo, const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 tabletId) const {
        AFL_VERIFY(originalTableInfo->AlterData);
        NKikimrTxColumnShard::TSchemaTxBody result = DoGetShardTxBody(originalTableInfo, original, tabletId);
        auto& alterResult = *result.MutableAlterTable();
        auto& alterInfo = *originalTableInfo->AlterData;
        if (alterInfo.Description.HasTtlSettings()) {
            *alterResult.MutableTtlSettings() = alterInfo.Description.GetTtlSettings();
        }
        if (alterInfo.Description.HasSchemaPresetVersionAdj()) {
            alterResult.SetSchemaPresetVersionAdj(alterInfo.Description.GetSchemaPresetVersionAdj());
        }
        return result;
    }

    bool Initialize(const TTablesStorage::TTableReadGuard& originalTableInfo, const TInitializationContext& iContext, IErrorCollector& errors) {
        return DoInitialize(originalTableInfo, iContext, errors);
    }

    bool CustomStartToSS(const TTablesStorage::TTableReadGuard& tableInfo, TStartSSContext& context, IErrorCollector& errors) {
        return DoCustomStartToSS(tableInfo, context, errors);
    }

    bool Deserialize(const NKikimrSchemeOp::TAlterColumnTable& alter, IErrorCollector& errors) {
        AlterRequest = alter;
        return DoInitializeFromProto(errors);
    }

    TColumnTableInfo::TPtr BuildTableInfo(const TTablesStorage::TTableExtractedGuard& tableInfo, IErrorCollector& errors) const {
        auto resultAlterData = TColumnTableInfo::BuildTableWithAlter(*tableInfo, AlterRequest);

        const NKikimrSchemeOp::TColumnTableSchema* tableSchema = GetTableSchema(tableInfo, errors);
        if (!tableSchema) {
            return nullptr;
        }

        TOlapSchema currentSchema;
        currentSchema.ParseFromLocalDB(*tableSchema);
        {
            auto c = PatchSchema(currentSchema);
            if (c.IsFail()) {
                errors.AddError(c.GetErrorMessage());
                return nullptr;
            } else if (*c) {
                NKikimrSchemeOp::TColumnTableSchema schemaUpdatedProto;
                currentSchema.Serialize(schemaUpdatedProto);
                *resultAlterData->Description.MutableSchema() = schemaUpdatedProto;
            }
        }

        if (!PatchTTL(currentSchema, *resultAlterData, errors)) {
            return nullptr;
        }

        return resultAlterData;
    }
};

}