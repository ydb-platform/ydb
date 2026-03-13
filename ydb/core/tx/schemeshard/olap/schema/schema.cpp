#include "schema.h"

#include <ydb/core/tx/schemeshard/common/validation.h>
#include <ydb/core/tx/schemeshard/olap/ttl/validator.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/minmax/meta.h>

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

void TOlapSchema::ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    NextColumnId = tableSchema.GetNextColumnId();
    Version = tableSchema.GetVersion();

    Columns.Parse(tableSchema);
    Indexes.Parse(tableSchema);
    Options.Parse(tableSchema);
}

void TOlapSchema::ParseIndexesFromFullSchema(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    Indexes.Parse(tableSchema);
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

bool TOlapSchema::AddDefaultMinMaxIndexes(IErrorCollector& errors) {
    NKikimrSchemeOp::TAlterColumnTableSchema alterSchema;
    for (auto& [id, col] : Columns.GetColumns()) {
        if (!NOlap::NIndexes::NMinMax::TIndexMeta::IsAvailableType(col.GetType())) {
            continue;
        }
        const TString indexName = "__minmax_" + col.GetName();
        if (Indexes.GetByName(indexName)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_default_minmax_index")("index_name", indexName)("reason", "index with " +indexName+ " already exists");
            continue;
        }
        auto* upsertIndex = alterSchema.AddUpsertIndexes();
        upsertIndex->SetName(indexName);
        upsertIndex->SetClassName(NOlap::NIndexes::NMinMax::TIndexMeta::GetClassNameStatic());
        upsertIndex->MutableMinMaxIndex()->SetColumnName(col.GetName());
    }
    if (alterSchema.UpsertIndexesSize() == 0) {
        return true;
    }
    TOlapIndexesUpdate indexUpdate;
    if (!indexUpdate.Parse(alterSchema, errors)) {
        return false;
    }
    return Indexes.ApplyUpdate(*this, indexUpdate, errors, NextColumnId);
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
