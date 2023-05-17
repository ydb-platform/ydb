#pragma once

#include "defs.h"
#include "schemeshard.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/scheme/scheme_types_proto.h>


namespace NKikimr::NSchemeShard {

    class IErrorCollector {
    public:
        virtual void AddError(const TEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) = 0;
        virtual void AddError(const TString& errorMsg) = 0;
    };

    class TProposeErrorCollector : public IErrorCollector {
        NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult& TxResult;
    public:
        TProposeErrorCollector(NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult& txResult)
            : TxResult(txResult)
        {}

        void AddError(const TEvSchemeShard::EStatus& errorStatus, const TString& errorMsg) override {
            TxResult.SetError(errorStatus, errorMsg);
        }

        void AddError(const TString& errorMsg) override {
            TxResult.SetError(NKikimrScheme::StatusSchemeError, errorMsg);
        }
    };

    class TOlapColumnSchema {
        YDB_ACCESSOR(ui32, Id, Max<ui32>());
        YDB_ACCESSOR(ui32, KeyOrder, Max<ui32>());

        YDB_READONLY_DEF(TString, Name);
        YDB_READONLY_DEF(TString, TypeName);
        YDB_READONLY_DEF(NScheme::TTypeInfo, Type);
        YDB_FLAG_ACCESSOR(NotNull, false);

    public:
        bool IsKeyColumn() const {
            return KeyOrder != Max<ui32>();
        }

        void Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const;
        void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema);
        bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema, IErrorCollector& errors);
    };

    class TOlapSchemaUpdate {
        YDB_READONLY_OPT(NKikimrSchemeOp::EColumnTableEngine, Engine);
        YDB_READONLY_DEF(TVector<TOlapColumnSchema>, Columns);
        YDB_READONLY_DEF(TVector<TString>, KeyColumnNames);

    public:
        bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors, bool allowNullKeys = false);
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
    };

    class TOlapSchema {
    public:
        using TColumn = TOlapColumnSchema;
        using TColumns = THashMap<ui32, TOlapColumnSchema>;
        using TColumnsByName = THashMap<TString, ui32>;

    private:
        YDB_READONLY_OPT(NKikimrSchemeOp::EColumnTableEngine, Engine);
        YDB_READONLY_DEF(TColumns, Columns);
        YDB_READONLY_DEF(TColumnsByName, ColumnsByName);
        YDB_READONLY_DEF(TVector<ui32>, KeyColumnIds);

        YDB_READONLY(ui32, NextColumnId, 1);
        YDB_READONLY(ui32, Version, 0);

    public:
        const TOlapColumnSchema* GetColumnByName(const TString& name) const noexcept {
            auto it = ColumnsByName.find(name);
            if (it != ColumnsByName.end()) {
                return &Columns.at(it->second);
            }
            return nullptr;
        }

        const TOlapColumnSchema* GetColumnById(const ui32 id) const noexcept {
            auto it = Columns.find(id);
            if (it != Columns.end()) {
                return &it->second;
            }
            return nullptr;
        }

        bool Update(const TOlapSchemaUpdate& schemaUpdate, IErrorCollector& errors);
        
        void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
        void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
        bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
        bool ValidateTtlSettings(const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings, IErrorCollector& errors) const;

        static bool IsAllowedType(ui32 typeId);
        static bool IsAllowedFirstPkType(ui32 typeId);
    };

    class TOlapStoreSchemaPreset : public TOlapSchema {
        YDB_ACCESSOR_DEF(TString, Name);
        YDB_ACCESSOR_DEF(ui32, Id);
        YDB_ACCESSOR(size_t, ProtoIndex, -1); // Preset index in the olap store description
    public:
        void Serialize(NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto) const;
        void ParseFromLocalDB(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto);
        bool ParseFromRequest(const NKikimrSchemeOp::TColumnTableSchemaPreset& presetProto, IErrorCollector& errors);
    };
}
