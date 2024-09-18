#include "schema.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NSchemeShard {

void TOlapColumnFamily::Serialize(NKikimrSchemeOp::TOlapColumnFamily& columnFamily) const {
    columnFamily.SetId(Id);
    TBase::Serialize(columnFamily);
}

void TOlapColumnFamily::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnFamily& columnFamily) {
    Id = columnFamily.GetId();
    TBase::ParseFromLocalDB(columnFamily);
}

const TOlapColumnFamily* TOlapColumnFamiliesDescription::GetById(const ui32 id) const noexcept {
    auto it = ColumnFamilies.find(id);
    if (it == ColumnFamilies.end()) {
        return nullptr;
    }
    return &it->second;
}

const TOlapColumnFamily* TOlapColumnFamiliesDescription::GetByIdVerified(const ui32 id) const noexcept {
    return TValidator::CheckNotNull(GetById(id));
}

const TOlapColumnFamily* TOlapColumnFamiliesDescription::GetByName(const TString& name) const noexcept {
    auto it = ColumnFamiliesByName.find(name);
    if (it.IsEnd()) {
        return nullptr;
    }
        return GetByIdVerified(it->second);
}

bool TOlapColumnFamiliesDescription::ApplyUpdate(
    const TOlapColumnFamiliesUpdate& schemaUpdate, IErrorCollector& errors, ui32& NextColumnFamilyId, THashSet<ui32>& alterColumnFamilyId) {
        for (const auto& family : schemaUpdate.GetAddColumnFamilies()) {
        auto familyName = family.GetName();
        if (!ColumnFamiliesByName.contains(familyName)) {
            TOlapColumnFamily columFamilyAdd(family, NextColumnFamilyId++);
            Y_ABORT_UNLESS(ColumnFamilies.emplace(columFamilyAdd.GetId(), columFamilyAdd).second);
            Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), columFamilyAdd.GetId()).second);
        } else {
            auto id = ColumnFamiliesByName[familyName];
            Y_ABORT_UNLESS(alterColumnFamilyId.insert(id).second);
            TOlapColumnFamlilyDiff columnFamilyDiff;
            columnFamilyDiff.SetName(familyName);
            columnFamilyDiff.SetSerializerContainer(family.GetSerializerContainer());
            if (!ColumnFamilies[id].ApplyDiff(columnFamilyDiff, errors)) {
                errors.AddError(TStringBuilder() << "Can't alter for column family: " << familyName);
                return false;
            }
        }
        }
    return true;
}

    bool TOlapColumnFamiliesDescription::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema) {
    for (const auto& family : tableSchema.GetColumnFamilies()) {
        TOlapColumnFamily columFamily;
        columFamily.ParseFromLocalDB(family);
        Y_ABORT_UNLESS(ColumnFamilies.emplace(columFamily.GetId(), columFamily).second);
        Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamily.GetName(), columFamily.GetId()).second);
    }
    return true;
    }

void TOlapColumnFamiliesDescription::Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const {
    for (const auto& [_, family] : ColumnFamilies) {
        family.Serialize(*tableSchema.AddColumnFamilies());
    }
}

}
