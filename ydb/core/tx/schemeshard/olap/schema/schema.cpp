#include "schema.h"

namespace NKikimr::NSchemeShard {

    bool TOlapSchema::Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors) {
        if (!Columns.ApplyUpdate(schemaUpdate.GetColumns(), errors, NextColumnId)) {
            return false;
        }

        if (!Indexes.ApplyUpdate(*this, schemaUpdate.GetIndexes(), errors, NextColumnId)) {
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
        CompositeMarksFlag = tableSchema.GetCompositeMarks();

        Columns.Parse(tableSchema);
        Indexes.Parse(tableSchema);
    }

    void TOlapSchema::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
        tableSchema.SetNextColumnId(NextColumnId);
        tableSchema.SetVersion(Version);
        tableSchema.SetCompositeMarks(CompositeMarksFlag);

        Y_ABORT_UNLESS(HasEngine());
        tableSchema.SetEngine(GetEngineUnsafe());

        Columns.Serialize(tableSchema);
        Indexes.Serialize(tableSchema);
    }

    bool TOlapSchema::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
        if (!Columns.Validate(opSchema, errors)) {
            return false;
        }

        if (!Indexes.Validate(opSchema, errors)) {
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
