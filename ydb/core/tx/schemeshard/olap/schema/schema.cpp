#include "schema.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/common/validation.h>
#include <ydb/core/tx/schemeshard/olap/ttl/validator.h>
#include <ydb/core/tx/tiering/rule/object.h>

namespace NKikimr::NSchemeShard {

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
            return TTTLValidator::ValidateColumnTableTtl(ttl.GetEnabled(), Indexes, {}, Columns.GetColumns(), Columns.GetColumnsByName(), errors);
        }
        case TTtlProto::kDisabled:
        default:
            break;
    }

    if (const TString& tieringId = ttl.GetUseTiering()) {
        const TPath path =
            TPath::Resolve(NColumnShard::NTiers::TTieringRule::GetBehaviour()->GetStorageTablePath(), &context)
                .Dive(ttl.GetUseTiering());
        {
            TPath::TChecker checks = path.Check();
            checks.NotEmpty().NotUnderDomainUpgrade().IsAtLocalSchemeShard().IsResolved().NotDeleted().IsTieringRule().NotUnderOperation();
            if (!checks) {
                errors.AddError(checks.GetStatus(), checks.GetError());
                return false;
            }
        }

        const auto* tieringRule = context.TieringRules.FindPtr(path.Base()->PathId);
        AFL_VERIFY(tieringRule)("name", tieringId);
        const auto* column = Columns.GetByName((*tieringRule)->DefaultColumn);
        if (!column) {
            errors.AddError("Incorrect tiering column - not found in scheme");
            return false;
        }
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

void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchemaExt) const {
    NKikimrSchemeOp::TColumnTableSchema resultLocal;
    resultLocal.SetNextColumnId(NextColumnId);
    resultLocal.SetVersion(Version);

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
