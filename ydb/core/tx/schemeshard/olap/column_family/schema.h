#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapColumnFamilyScheme: public TOlapColumnFamlilyAdd {
private:
    using TBase = TOlapColumnFamlilyAdd;

public:
    TOlapColumnFamilyScheme() = default;

    TOlapColumnFamilyScheme(const TOlapColumnFamlilyAdd& base)
        : TBase(base)
    {
    }

    void Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const;
    void ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily);
};

class TOlapColumnFamiliesDescription {
public:
private:
    using TOlapColumnFamily = TOlapColumnFamilyScheme;
    using TOlapColumnFamilies = TVector<TOlapColumnFamily>;
    using TOlapColumnFamiliesByName = THashMap<TString, ui32>;

    YDB_READONLY_DEF(TOlapColumnFamilies, ColumnFamilies);
    YDB_READONLY_DEF(TOlapColumnFamiliesByName, ColumnFamiliesByName);

public:
    const TOlapColumnFamilyScheme* GetFamilyById(const ui32 id) const noexcept;
    const TOlapColumnFamilyScheme* GetByIdVerified(const ui32 id) const noexcept;
    const TOlapColumnFamilyScheme* GetFamilyByName(const TString& name) const noexcept;

    bool ApplyUpdate(const TOlapColumnFamiliesUpdate& schemaUpdate, IErrorCollector& errors);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
};
}
