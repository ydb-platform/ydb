#include "validator.h"
#include <ydb/core/tx/schemeshard/common/validation.h>

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

bool TTTLValidator::ValidateColumnTableTtl(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl, const TOlapIndexesDescription& indexes, const THashMap<ui32, TOlapColumnsDescription::TColumn>& sourceColumns, const THashMap<ui32, TOlapColumnsDescription::TColumn>& alterColumns, const THashMap<TString, ui32>& colName2Id, IErrorCollector& errors) {
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

    if (!ttl.HasExpireAfterSeconds() && ttl.GetTiers().empty()) {
        errors.AddError("TTL without eviction time");
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

    return true;
}

}
