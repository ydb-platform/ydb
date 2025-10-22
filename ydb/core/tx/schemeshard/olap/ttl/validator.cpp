#include "validator.h"

#include <ydb/core/tx/schemeshard/common/validation.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr::NSchemeShard {

namespace {
static inline bool IsDropped(const TOlapColumnsDescription::TColumn& col) {
    Y_UNUSED(col);
    return false;
}

static inline NScheme::TTypeInfo GetType(const TOlapColumnsDescription::TColumn& col) {
    return col.GetType();
}

}

bool TTTLValidator::ValidateColumnTableTtl(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl, const TOlapIndexesDescription& indexes, const THashMap<ui32, TOlapColumnsDescription::TColumn>& sourceColumns, const THashMap<ui32, TOlapColumnsDescription::TColumn>& alterColumns, const THashMap<TString, ui32>& colName2Id, const TOperationContext& context, IErrorCollector& errors) {
    const TString colName = ttl.GetColumnName();

    auto it = colName2Id.find(colName);
    if (it == colName2Id.end()) {
        errors.AddError(Sprintf("Cannot enable TTL on unknown column: '%s'", colName.data()));
        return false;
    }

    const TOlapColumnsDescription::TColumn* column = nullptr;
    const ui32 colId = it->second;
    if (alterColumns.contains(colId)) {
        column = &alterColumns.at(colId);
    } else if (sourceColumns.contains(colId)) {
        column = &sourceColumns.at(colId);
    } else {
        Y_ABORT_UNLESS("Unknown column");
    }

    if (IsDropped(*column)) {
        errors.AddError(Sprintf("Cannot enable TTL on dropped column: '%s'", colName.data()));
        return false;
    }

    if (ttl.HasExpireAfterBytes()) {
        errors.AddError("TTL with eviction by size is not supported yet");
        return false;
    }

    auto unit = ttl.GetColumnUnit();

    const auto& columnType = GetType(*column);
    switch (columnType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::Pg:
            errors.AddError("Unsupported column type for TTL in column tables");
            return false;
        default:
            break;
    }

    TString errStr;
    if (!NValidation::TTTLValidator::ValidateUnit(columnType, unit, errStr)) {
        errors.AddError(errStr);
        return false;
    }
    if (!NValidation::TTTLValidator::ValidateTiers(ttl.GetTiers(), errStr)) {
        errors.AddError(errStr);
        return false;
    }
    if (!AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        for (const auto& tier : ttl.GetTiers()) {
            if (tier.HasEvictToExternalStorage()) {
                errors.AddError(NKikimrScheme::StatusPreconditionFailed, "Tiering functionality is disabled for OLAP tables");
                return false;
            }
        }
    }
    {
        bool correct = false;
        if (column->GetKeyOrder() && *column->GetKeyOrder() == 0) {
            correct = true;
        } else {
            for (auto&& [_, i] : indexes.GetIndexes()) {
                auto proto = i.GetIndexMeta().SerializeToProto();
                if (proto.HasMaxIndex() && proto.GetMaxIndex().GetColumnId() == column->GetId()) {
                    correct = true;
                    break;
                }
            }
        }
        if (!correct) {
            errors.AddError("Haven't MAX-index for TTL column and TTL column is not first column in primary key");
            return false;
        }
    }

    for (const auto& tier : ttl.GetTiers()) {
        if (!tier.HasEvictToExternalStorage()) {
            continue;
        }
        const TString& tierPathString = tier.GetEvictToExternalStorage().GetStorage();
        TPath tierPath = TPath::Resolve(tierPathString, context.SS);
        if (!tierPath.IsResolved() || tierPath.IsDeleted() || tierPath.IsUnderDeleting()) {
            errors.AddError("Object not found: " + tierPathString);
            return false;
        }
        if (!tierPath->IsExternalDataSource()) {
            errors.AddError("Not an external data source: " + tierPathString);
            return false;
        }
        {
            auto* findExternalDataSource = context.SS->ExternalDataSources.FindPtr(tierPath->PathId);
            AFL_VERIFY(findExternalDataSource);
            NKikimrSchemeOp::TExternalDataSourceDescription proto;
            (*findExternalDataSource)->FillProto(proto, false);
            if (auto status = NColumnShard::NTiers::TTierConfig().DeserializeFromProto(proto); status.IsFail()) {
                errors.AddError("Cannot use external data source \"" + tierPathString + "\" for tiering: " + status.GetErrorMessage());
                return false;
            }
        }
    }

    return true;
}

}
