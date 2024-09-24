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
            ui32 index = 0;
            if (family.GetName() != "default") {
                index = NextColumnFamilyId++;
            }
            TOlapColumnFamily columFamilyAdd(family, index);
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

bool TOlapColumnFamiliesDescription::Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    ui32 lastColumnFamilyId = 0;
    THashSet<ui32> usedColumnFamilies;
    for (const auto& familyProto : opSchema.GetColumnFamilies()) {
        if (familyProto.GetName().Empty()) {
            errors.AddError("Column family can't have an empty name");
            return false;
        }

        const TString& columnFamilyName = familyProto.GetName();
        auto* family = GetByName(columnFamilyName);
        if (!family) {
            errors.AddError("Column family '" + columnFamilyName + "' does not match schema preset");
            return false;
        }

        if (familyProto.HasId() && familyProto.GetId() != family->GetId()) {
            errors.AddError("Column family '" + columnFamilyName + "' has id " + familyProto.GetId() + " that does not match schema preset");
            return false;
        }

        if (!usedColumnFamilies.insert(family->GetId()).second) {
            errors.AddError("Column family '" + columnFamilyName + "' is specified multiple times");
            return false;
        }

        if (familyProto.GetId() < lastColumnFamilyId) {
            errors.AddError("Column family order does not match schema preset");
            return false;
        }
        lastColumnFamilyId = familyProto.GetId();

        if (!familyProto.HasSerializer()) {
            errors.AddError("Missing serializer for column family '" + columnFamilyName + "'");
            return false;
        }

        NArrow::NSerialization::TSerializerContainer serializer;
        AFL_VERIFY(serializer.DeserializeFromProto(familyProto.GetSerializer()));
        if (!family->GetSerializerContainer().IsEqualTo(serializer)) {
            errors.AddError("Compression from column family '" + columnFamilyName + "` is not matching schema preset");
            return false;
        }
    }

    for (const auto& [_, family] : ColumnFamilies) {
        if (!usedColumnFamilies.contains(family.GetId())) {
            errors.AddError("Specified schema is missing some schema preset column families");
            return false;
        }
    }

    return true;
}
}
