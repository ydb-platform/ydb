#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapColumnSchema: public TOlapColumnAdd {
private:
    using TBase = TOlapColumnAdd;
    YDB_READONLY(ui32, Id, Max<ui32>());
public:
    TOlapColumnSchema(const TOlapColumnAdd& base, const ui32 id)
        : TBase(base)
        , Id(id) {

    }
    TOlapColumnSchema(const std::optional<ui32>& keyOrder)
        : TBase(keyOrder) {

    }
    void Serialize(NKikimrSchemeOp::TOlapColumnDescription& columnSchema) const;
    void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnDescription& columnSchema);
};

class TOlapColumnsDescription {
public:
    using TColumn = TOlapColumnSchema;
    using TColumns = THashMap<ui32, TOlapColumnSchema>;
    using TColumnsByName = THashMap<TString, ui32>;

private:
    YDB_READONLY_DEF(TColumns, Columns);
    YDB_READONLY_DEF(TColumnsByName, ColumnsByName);
    YDB_READONLY_DEF(TVector<ui32>, KeyColumnIds);

public:
    const TOlapColumnSchema* GetByName(const TString& name) const noexcept {
        auto it = ColumnsByName.find(name);
        if (it != ColumnsByName.end()) {
            return GetByIdVerified(it->second);
        }
        return nullptr;
    }

    const TOlapColumnSchema* GetById(const ui32 id) const noexcept {
        auto it = Columns.find(id);
        if (it != Columns.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const TOlapColumnSchema* GetByIdVerified(const ui32 id) const noexcept;

    bool ApplyUpdate(const TOlapColumnsUpdate& schemaUpdate, IErrorCollector& errors, ui32& nextEntityId);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
