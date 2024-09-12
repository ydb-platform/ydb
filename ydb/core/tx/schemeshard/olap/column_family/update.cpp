#include "update.h"

namespace NKikimr::NSchemeShard {

bool TOlapColumnFamlilyDiff::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamilyDiff& diffColumnFamily, IErrorCollector& errors) {
    if (!diffColumnFamily.HasName()) {
        errors.AddError("Column family: empty field name");
        return false;
    }
    if (!diffColumnFamily.HasColumnCodec()) {
        errors.AddError("Column family: empty field codec");
        return false;
    }

    Name = diffColumnFamily.GetName();
    Codec = diffColumnFamily.GetColumnCodec();
    return true;
}

bool TOlapColumnFamlilyAdd::ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& columnFamily, IErrorCollector& errors) {
    // if (!columnFamily.HasId()) {
    //     errors.AddError("Column family: empty field Id");
    //     return false;
    // }

    if (!columnFamily.HasName()) {
        errors.AddError("Column family: empty field Name");
        return false;
    }

    if (!columnFamily.HasColumnCodec()) {
        errors.AddError("Column family: empty field Codec");
        return false;
    }

    // Id = columnFamily.GetId();
    Name = columnFamily.GetName();
    Codec = columnFamily.GetColumnCodec();
    return true;
}

void TOlapColumnFamlilyAdd::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
    // Id = columnFamily.GetId();
    Name = columnFamily.GetName();
    Codec = columnFamily.GetColumnCodec();
}

void TOlapColumnFamlilyAdd::Serialize(NKikimrSchemeOp::TFamilyDescription& familyDescription) const {
    // familyDescription.SetId(Id);
    familyDescription.SetName(Name);
    familyDescription.SetColumnCodec(Codec);
}

bool TOlapColumnFamlilyAdd::ApplyDiff(const TOlapColumnFamlilyDiff& diffColumnFamily, IErrorCollector& errors) {
    Y_ABORT_UNLESS(GetName() == diffColumnFamily.GetName());
    if (!diffColumnFamily.GetCodec()) {
        errors.AddError("Column family: empty field Codec");
        return false;
    }
    Codec = diffColumnFamily.GetCodec();
    return true;
}

bool TOlapColumnFamiliesUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors) {
    TSet<TString> familyNames;
    for (auto&& family : tableSchema.GetColumnFamilies()) {
        auto familyName = family.GetName();
        if (!familyNames.emplace(familyName).second) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Duplicate column family '" << familyName << "'");
            return false;
        }
        TOlapColumnFamlilyAdd columnFamily;
        if (!columnFamily.ParseFromRequest(family, errors)) {
            return false;
        }
        familyNames.insert(familyName);
        AddColumnFamilies.emplace_back(columnFamily);
    }

    return true;
}

bool TOlapColumnFamiliesUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
    TSet<TString> addColumnFamilies;
    for (auto&& family : alterRequest.GetAddColumnFamily()) {
        auto familyName = family.GetName();
        if (!addColumnFamilies.emplace(familyName).second) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Duplicate column family '" << familyName << "'");
            return false;
        }
        TOlapColumnFamlilyAdd columnFamily({});
        if (!columnFamily.ParseFromRequest(family, errors)) {
            return false;
        }
        addColumnFamilies.insert(familyName);
        AddColumnFamilies.emplace_back(columnFamily);
    }

    for (auto&& family : alterRequest.GetAlterColumnFamily()) {
        TOlapColumnFamlilyDiff columnFamily({});
        if (!columnFamily.ParseFromRequest(family, errors)) {
            return false;
        }
        AlterColumnFamily.emplace_back(columnFamily);
    }
    return true;
}
}
