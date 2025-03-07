#include "schema.h"

#include <ydb/core/tx/schemeshard/olap/column_family/column_family.h>

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NSchemeShard {

void TOlapColumnFamily::Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const {
    columnFamily.SetId(Id);
    TBase::Serialize(columnFamily);
}

void TOlapColumnFamily::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
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
    const TOlapColumnFamiliesUpdate& schemaUpdate, IErrorCollector& errors, ui32& NextColumnFamilyId) {
    for (auto&& family : schemaUpdate.GetAddColumnFamilies()) {
        auto familyName = family.GetName();
        if (ColumnFamiliesByName.contains(familyName)) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists, TStringBuilder() << "column family '" << familyName << "' already exists");
            return false;
        }
        ui32 index = 0;
        if (familyName != "default") {
            index = NextColumnFamilyId++;
        }
        TOlapColumnFamily columFamilyAdd(family, index);
        Y_ABORT_UNLESS(ColumnFamilies.emplace(columFamilyAdd.GetId(), columFamilyAdd).second);
        Y_ABORT_UNLESS(ColumnFamiliesByName.emplace(columFamilyAdd.GetName(), columFamilyAdd.GetId()).second);
    }

    for (auto&& family : schemaUpdate.GetAlterColumnFamily()) {
        auto familyName = family.GetName();
        auto it = ColumnFamiliesByName.find(familyName);
        if (it.IsEnd()) {
            errors.AddError(
                NKikimrScheme::StatusSchemeError, TStringBuilder() << "column family '" << familyName << "' not exists for altering");
            return false;
        } else {
            auto itColumnFamily = ColumnFamilies.find(it->second);
            Y_ABORT_UNLESS(itColumnFamily != ColumnFamilies.end());
            Y_ABORT_UNLESS(AlterColumnFamiliesId.insert(it->second).second);
            TOlapColumnFamily& alterColumnFamily = itColumnFamily->second;
            if (!alterColumnFamily.ApplyDiff(family, errors)) {
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

bool TOlapColumnFamiliesDescription::ValidateForStore(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const {
    ui32 lastColumnFamilyId = 0;
    THashSet<ui32> usedColumnFamilies;
    for (const auto& familyProto : opSchema.GetColumnFamilies()) {
        if (familyProto.GetName().empty()) {
            errors.AddError("column family can't have an empty name");
            return false;
        }

        const TString& columnFamilyName = familyProto.GetName();
        auto* family = GetByName(columnFamilyName);
        if (!family) {
            errors.AddError("column family '" + columnFamilyName + "' does not match schema preset");
            return false;
        }

        if (familyProto.HasId() && familyProto.GetId() != family->GetId()) {
            errors.AddError("column family '" + columnFamilyName + "' has id " + familyProto.GetId() + " that does not match schema preset");
            return false;
        }

        if (!usedColumnFamilies.insert(family->GetId()).second) {
            errors.AddError("column family '" + columnFamilyName + "' is specified multiple times");
            return false;
        }

        if (familyProto.GetId() < lastColumnFamilyId) {
            errors.AddError("column family order does not match schema preset");
            return false;
        }
        lastColumnFamilyId = familyProto.GetId();

        if (familyProto.HasColumnCodec() && family->GetSerializerContainer().HasObject()) {
            TColumnFamily columnFamily;
            auto result = columnFamily.DeserializeFromProto(familyProto);
            if (result.IsFail()) {
                errors.AddError(result.GetErrorMessage());
                return false;
            }
            auto serializerProto = columnFamily.GetSerializer();
            if (serializerProto.IsFail()) {
                errors.AddError(serializerProto.GetErrorMessage());
                return false;
            }
            NArrow::NSerialization::TSerializerContainer serializer;
            if (!serializer.DeserializeFromProto(serializerProto.GetResult())) {
                errors.AddError(TStringBuilder() << "can't deserialize column family `" << columnFamilyName << "`  from proto ");
                return false;
            }
            if (!family->GetSerializerContainer().IsEqualTo(serializer)) {
                errors.AddError(TStringBuilder() << "compression from column family '" << columnFamilyName << "` is not matching schema preset");
                return false;
            }
        } else if ((!familyProto.HasColumnCodec() && family->GetSerializerContainer().HasObject()) ||
                   (familyProto.HasColumnCodec() && !family->GetSerializerContainer().HasObject())) {
            errors.AddError(TStringBuilder() << "compression is not matching schema preset in column family `" << columnFamilyName << "`");
            return false;
        }
    }

    for (const auto& [_, family] : ColumnFamilies) {
        if (!usedColumnFamilies.contains(family.GetId())) {
            errors.AddError("specified schema is missing some schema preset column families");
            return false;
        }
    }

    return true;
}
}  // namespace NKikimr::NSchemeShard
