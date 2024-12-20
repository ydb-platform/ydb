#include "schema.h"

#include <ydb/core/tx/schemeshard/common/validation.h>
#include <ydb/core/tx/schemeshard/olap/ttl/validator.h>

namespace NKikimr::NSchemeShard {

bool TOlapSchema::ValidateTtlSettings(
    const NKikimrSchemeOp::TColumnDataLifeCycle& ttl, const TOperationContext& context, IErrorCollector& errors) const {
    using TTtlProto = NKikimrSchemeOp::TColumnDataLifeCycle;
    switch (ttl.GetStatusCase()) {
        case TTtlProto::kEnabled: 
        {
            const auto* column = Columns.GetByName(ttl.GetEnabled().GetColumnName());
            if (!column) {
                errors.AddError("Incorrect ttl column - not found in scheme");
                return false;
            }
            return TTTLValidator::ValidateColumnTableTtl(ttl.GetEnabled(), Indexes, {}, Columns.GetColumns(), Columns.GetColumnsByName(), context, errors);
        }
        case TTtlProto::kDisabled:
        default:
            break;
    }

    return true;
}

bool TOlapSchema::Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
    if (!ColumnFamilies.ApplyUpdate(schemaUpdate.GetColumnFamilies(), errors, NextColumnFamilyId)) {
        return false;
    }

    if (!Columns.ApplyUpdate(schemaUpdate.GetColumns(), ColumnFamilies, errors, NextColumnId)) {
        return false;
    }

    if (!Indexes.ApplyUpdate(*this, schemaUpdate.GetIndexes(), errors, NextColumnId)) {
        return false;
    }

    if (!Options.ApplyUpdate(schemaUpdate.GetOptions(), errors)) {
        return false;
    }

    ++Version;
    return true;
}

void TOlapSchema::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    NextColumnId = tableSchema.GetNextColumnId();
    NextColumnFamilyId = tableSchema.GetNextColumnFamilyId();
    Version = tableSchema.GetVersion();

    ColumnFamilies.Parse(tableSchema);
    Columns.Parse(tableSchema);
    Indexes.Parse(tableSchema);
    Options.Parse(tableSchema);
}

void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchemaExt) const {
    NKikimrSchemeOp::TColumnTableSchema resultLocal;
    resultLocal.SetNextColumnId(NextColumnId);
    resultLocal.SetNextColumnFamilyId(NextColumnFamilyId);
    resultLocal.SetVersion(Version);

    ColumnFamilies.Serialize(resultLocal);
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
