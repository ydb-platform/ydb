#pragma once
#include <ydb/core/tx/schemeshard/olap/columns/update.h>
#include <ydb/core/tx/schemeshard/olap/indexes/update.h>
#include <ydb/core/tx/schemeshard/olap/columns/schema.h>
#include <ydb/core/tx/schemeshard/olap/indexes/schema.h>
#include <ydb/core/tx/schemeshard/olap/options/schema.h>
#include "update.h"

namespace NKikimr::NSchemeShard {

    class TOlapSchema {
    private:
        YDB_READONLY_OPT(NKikimrSchemeOp::EColumnTableEngine, Engine);
        YDB_READONLY_DEF(TOlapColumnsDescription, Columns);
        YDB_READONLY_DEF(TOlapIndexesDescription, Indexes);
        YDB_READONLY_DEF(TOlapOptionsDescription, Options);

        YDB_READONLY(ui32, NextColumnId, 1);
        YDB_READONLY(ui32, Version, 0);

    public:
        bool Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors);

        void ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
        void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
        bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
        bool ValidateTtlSettings(const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings, IErrorCollector& errors) const;
    };

    class TOlapStoreSchemaPreset: public TOlapSchema {
    private:
        using TBase = TOlapSchema;
        YDB_ACCESSOR_DEF(TString, Name);
        YDB_ACCESSOR_DEF(ui32, Id);
        YDB_ACCESSOR(size_t, ProtoIndex, -1); // Preset index in the olap store description
    public:
        void Serialize(NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) const;
        void ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto);
        bool ParseFromRequest(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto, IErrorCollector& errors);
    };
}
