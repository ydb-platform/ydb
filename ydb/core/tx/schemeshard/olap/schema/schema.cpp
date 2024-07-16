#include "schema.h"
#include <ydb/core/tx/schemeshard/common/validation.h>

namespace NKikimr::NSchemeShard {

namespace {
static inline bool IsDropped(const TOlapColumnsDescription::TColumn& col) {
    Y_UNUSED(col);
    return false;
}

static inline ui32 GetType(const TOlapColumnsDescription::TColumn& col) {
    Y_ABORT_UNLESS(col.GetType().GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
    return col.GetType().GetTypeId();
}

}

static bool ValidateColumnTableTtl(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl,
    const THashMap<ui32, TOlapColumnsDescription::TColumn>& sourceColumns,
    const THashMap<ui32, TOlapColumnsDescription::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    IErrorCollector& errors) {
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

    if (!ttl.HasExpireAfterSeconds()) {
        errors.AddError("TTL without eviction time");
        return false;
    }

    auto unit = ttl.GetColumnUnit();

    switch (GetType(*column)) {
        case NScheme::NTypeIds::DyNumber:
            errors.AddError("Unsupported column type for TTL in column tables");
            return false;
        default:
            break;
    }

    TString errStr;
    if (!NValidation::TTTLValidator::ValidateUnit(GetType(*column), unit, errStr)) {
        errors.AddError(errStr);
        return false;
    }
    return true;
}

bool TOlapSchema::ValidateTtlSettings(const NKikimrSchemeOp::TColumnDataLifeCycle& ttl, IErrorCollector& errors) const {
    using TTtlProto = NKikimrSchemeOp::TColumnDataLifeCycle;
    switch (ttl.GetStatusCase()) {
        case TTtlProto::kEnabled:
        {
            const auto* column = Columns.GetByName(ttl.GetEnabled().GetColumnName());
            if (!column) {
                errors.AddError("Incorrect ttl column - not found in scheme");
                return false;
            }
            return ValidateColumnTableTtl(ttl.GetEnabled(), {}, Columns.GetColumns(), Columns.GetColumnsByName(), errors);
        }
        case TTtlProto::kDisabled:
        default:
            break;
    }

    return true;
}

bool TOlapSchema::Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
    if (!Columns.ApplyUpdate(schemaUpdate.GetColumns(), errors, NextColumnId)) {
        return false;
    }

    if (!Indexes.ApplyUpdate(*this, schemaUpdate.GetIndexes(), errors, NextColumnId)) {
        return false;
    }

    if (!Options.ApplyUpdate(schemaUpdate.GetOptions(), errors)) {
        return false;
    }

    if (!HasEngine()) {
        Engine = schemaUpdate.GetEngineDef(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
    } else {
        if (schemaUpdate.HasEngine()) {
            errors.AddError(NKikimrScheme::StatusSchemeError, "No engine updates supported");
            return false;
        }
    }

    ++Version;
    return true;
}

void TOlapSchema::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    NextColumnId = tableSchema.GetNextColumnId();
    Version = tableSchema.GetVersion();
    Y_ABORT_UNLESS(tableSchema.HasEngine());
    Engine = tableSchema.GetEngine();

    Columns.Parse(tableSchema);
    Indexes.Parse(tableSchema);
    Options.Parse(tableSchema);
}

void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchemaExt) const {
    NKikimrSchemeOp::TColumnTableSchema resultLocal;
    resultLocal.SetNextColumnId(NextColumnId);
    resultLocal.SetVersion(Version);

    Y_ABORT_UNLESS(HasEngine());
    resultLocal.SetEngine(GetEngineUnsafe());

    Columns.Serialize(resultLocal);
    Indexes.Serialize(resultLocal);
    Options.Serialize(resultLocal);
    std::swap(resultLocal, tableSchemaExt);
}

bool TOlapSchema::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    if (!Columns.Validate(opSchema, errors)) {
        return false;
    }

    if (!Indexes.Validate(opSchema, errors)) {
        return false;
    }

    if (!Options.Validate(opSchema, errors)) {
        return false;
    }

    if (opSchema.GetEngine() != Engine) {
        errors.AddError("Specified schema engine does not match schema preset");
        return false;
    }
    return true;
}

void TOlapStoreSchemaPreset::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) {
    Y_ABORT_UNLESS(presetProto.HasId());
    Y_ABORT_UNLESS(presetProto.HasName());
    Y_ABORT_UNLESS(presetProto.HasSchema());
    Id = presetProto.GetId();
    Name = presetProto.GetName();
    TOlapSchema::ParseFromLocalDB(presetProto.GetSchema());
}

void TOlapStoreSchemaPreset::Serialize(NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) const {
    presetProto.SetId(Id);
    presetProto.SetName(Name);
    TOlapSchema::Serialize(*presetProto.MutableSchema());
}

bool TOlapStoreSchemaPreset::ParseFromRequest(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto, IErrorCollector& errors) {
    if (presetProto.HasId()) {
        errors.AddError("Schema preset id cannot be specified explicitly");
        return false;
    }
    if (!presetProto.GetName()) {
        errors.AddError("Schema preset name cannot be empty");
        return false;
    }
    Name = presetProto.GetName();
    return true;
}
}
