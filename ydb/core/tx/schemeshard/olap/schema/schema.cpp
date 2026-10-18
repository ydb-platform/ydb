#include "schema.h"

#include <ydb/core/tx/schemeshard/common/validation.h>

namespace NKikimr::NSchemeShard {

namespace {

void AdvanceNextColumnIdPastIndexes(ui32& nextColumnId, const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    for (const auto& indexProto : tableSchema.GetIndexes()) {
        nextColumnId = Max(nextColumnId, indexProto.GetId() + 1);
    }
}

} // namespace

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

    ++Version;
    return true;
}

bool TOlapSchema::ParseFromProto(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys) {
    TOlapSchemaUpdate schemaDiff;
    if (!schemaDiff.Parse(tableSchema, errors, allowNullKeys)) {
        return false;
    }
    if (!Update(schemaDiff, errors)) {
        return false;
    }
    ParseIndexesFromFullSchema(tableSchema);
    if (!Indexes.ValidateNoDuplicateMinMaxAndBloomFilterIndexes(*this, errors)) {
        return false;
    }
    return true;
}

void TOlapSchema::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    NextColumnId = tableSchema.GetNextColumnId();
    Version = tableSchema.GetVersion();

    Columns.Parse(tableSchema);
    Indexes.Parse(tableSchema);
    Options.Parse(tableSchema);
    AdvanceNextColumnIdPastIndexes(NextColumnId, tableSchema);
}

void TOlapSchema::ParseIndexesFromFullSchema(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    Indexes.Parse(tableSchema);
    AdvanceNextColumnIdPastIndexes(NextColumnId, tableSchema);
}

void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchemaExt) const {
    NKikimrSchemeOp::TColumnTableSchema resultLocal;
    resultLocal.SetNextColumnId(NextColumnId);
    resultLocal.SetVersion(Version);

    Columns.Serialize(resultLocal);
    Indexes.Serialize(resultLocal);
    Options.Serialize(resultLocal);
    std::swap(resultLocal, tableSchemaExt);
}

bool TOlapSchema::ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    if (!Columns.ValidateForStore(opSchema, errors)) {
        return false;
    }

    if (!Indexes.ValidateForStore(opSchema, errors)) {
        return false;
    }

    if (!Options.ValidateForStore(opSchema, errors)) {
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
