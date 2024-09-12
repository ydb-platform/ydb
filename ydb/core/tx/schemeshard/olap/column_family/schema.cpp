#include "schema.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NSchemeShard {

void TOlapColumnFamilyScheme::Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const {
    TBase::Serialize(columnFamily);
}

void TOlapColumnFamilyScheme::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
    TBase::ParseFromLocalDB(columnFamily);
}

const TOlapColumnFamilyScheme* TOlapColumnFamiliesDescription::GetFamilyById(const ui32 id) const noexcept {
    if (id < ColumnFamilies.size()) {
        return &ColumnFamilies[id];
    }
    return nullptr;
}

const NKikimr::NSchemeShard::TOlapColumnFamilyScheme* TOlapColumnFamiliesDescription::GetByIdVerified(const ui32 id) const noexcept {
    return TValidator::CheckNotNull(GetFamilyById(id));
}

const TOlapColumnFamilyScheme* TOlapColumnFamiliesDescription::GetFamilyByName(const TString& name) const noexcept {
    auto it = ColumnFamiliesByName.find(name);
    if (it != ColumnFamiliesByName.end()) {
        return GetByIdVerified(it->second);
    }
    return nullptr;
}

bool TOlapColumnFamiliesDescription::ApplyUpdate(const TOlapColumnFamiliesUpdate& schemaUpdate, IErrorCollector& errors) {
    for (const auto& family : schemaUpdate.GetAddColumnFamilies()) {
        auto familyName = family.GetName();
        if (ColumnFamiliesByName.contains(familyName)) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "family '" << familyName << "' already exists");
            return false;
        }
        TOlapColumnFamily columFamilyAdd(family);
        ColumnFamilies.emplace_back(columFamilyAdd);
        // Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), columFamilyAdd.GetId()).second);
        Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), ColumnFamilies.size() - 1).second);
    }
    return true;
}

void TOlapColumnFamiliesDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    for (const auto& family : tableSchema.GetColumnFamilies()) {
        TOlapColumnFamily columFamilyAdd;
        columFamilyAdd.ParseFromLocalDB(family);
        ColumnFamilies.emplace_back(columFamilyAdd);
        // Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), columFamilyAdd.GetId()).second);
        Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), ColumnFamilies.size() - 1).second);
    }
}

void TOlapColumnFamiliesDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& family : ColumnFamilies) {
        family.Serialize(*tableSchema.AddColumnFamilies());
    }
}

}
