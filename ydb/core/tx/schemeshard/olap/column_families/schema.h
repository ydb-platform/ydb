#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapColumnFamily: public TOlapColumnFamlilyAdd {
private:
    using TBase = TOlapColumnFamlilyAdd;
    YDB_READONLY(ui32, Id, Max<ui32>());

public:
    TOlapColumnFamily() = default;
    TOlapColumnFamily(const TOlapColumnFamlilyAdd& base, ui32 Id)
        : TBase(base)
        , Id(Id)
    {
    }

    void Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const;
    void ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily);
};

class TOlapColumnFamiliesDescription {
public:
private:
    using TOlapColumnFamilies = TMap<ui32, TOlapColumnFamily>;
    using TOlapColumnFamiliesByName = THashMap<TString, ui32>;

    YDB_READONLY_DEF(TOlapColumnFamilies, ColumnFamilies);
    YDB_READONLY_DEF(TOlapColumnFamiliesByName, ColumnFamiliesByName);
    YDB_READONLY_DEF(THashSet<ui32>, AlterColumnFamiliesId);

public:
    const TOlapColumnFamily* GetById(const ui32 id) const noexcept;
    const TOlapColumnFamily* GetByIdVerified(const ui32 id) const noexcept;
    const TOlapColumnFamily* GetByName(const TString& name) const noexcept;

    bool ApplyUpdate(const TOlapColumnFamiliesUpdate& schemaUpdate, IErrorCollector& errors, ui32& NextColumnFamilyId);

    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
