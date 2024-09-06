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
    if (!columnFamily.HasName()) {
        errors.AddError("Column family: empty field name");
        return false;
    }

    if (!columnFamily.HasColumnCodec()) {
        errors.AddError("Column family: empty field codec");
        return false;
    }

    Name = columnFamily.GetName();
    Codec = columnFamily.GetColumnCodec();
    return true;
}

void TOlapColumnFamlilyAdd::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
    Name = columnFamily.GetName();
    Codec = columnFamily.GetColumnCodec();
}

void TOlapColumnFamlilyAdd::Serialize(NKikimrSchemeOp::TFamilyDescription& familyDescription) const {
    familyDescription.SetName(Name);
    familyDescription.SetColumnCodec(Codec);
}

bool TOlapColumnFamlilyAdd::ApplyDiff(const TOlapColumnFamlilyDiff& diffColumnFamily, IErrorCollector& errors) {
    Y_ABORT_UNLESS(GetName() == diffColumnFamily.GetName());
    if (!diffColumnFamily.GetCodec()) {
        errors.AddError("Column family: empty field codec");
        return false;
    }
    Codec = diffColumnFamily.GetCodec();
    return true;
}

bool TOlapColumnFamiliesUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors) {
    TSet<TString> familyNames;
    for (auto&& family : tableSchema.GetColumnFamilies()) {
        if (!familyNames.emplace(family.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Duplicate column family '" << family.GetName() << "'");
            return false;
        }
        TOlapColumnFamlilyAdd columnFamily({});
        if (!columnFamily.ParseFromRequest(family, errors)) {
            return false;
        }
        familyNames.insert(family.GetName());
        AddColumnFamilies.emplace_back(columnFamily);
    }

    return true;
}

bool TOlapColumnFamiliesUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors) {
    TSet<TString> addColumnFamilies;
    for (auto&& family : alterRequest.GetAddColumnFamily()) {
        if (!addColumnFamilies.emplace(family.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Duplicate column family '" << family.GetName() << "'");
            return false;
        }
        TOlapColumnFamlilyAdd columnFamily({});
        if (!columnFamily.ParseFromRequest(family, errors)) {
            return false;
        }
        addColumnFamilies.insert(family.GetName());
        AddColumnFamilies.emplace_back(columnFamily);
    }
    return true;
}
}
