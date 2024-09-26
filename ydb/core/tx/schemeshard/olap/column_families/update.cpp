#include "update.h"

#include <ydb/core/formats/arrow/serializer/native.h>

namespace NKikimr::NSchemeShard {

NKikimr::TConclusion<std::shared_ptr<NArrow::NSerialization::ISerializer>> TOlapColumnFamilyHelper::ParseSerializer(
    const NKikimrSchemeOp::TOlapColumn::TSerializer& serializer) {
    std::shared_ptr<NArrow::NSerialization::ISerializer> Serializer;
    if (serializer.GetClassName() == NArrow::NSerialization::TNativeSerializer::GetClassNameStatic()) {
        Serializer = std::make_shared<NArrow::NSerialization::TNativeSerializer>();
        if (Serializer->DeserializeFromProto(serializer).IsFail()) {
            return NKikimr::TConclusionStatus::Fail("Can't parse serializer in TOlapColumnFamily");
        }
    } else {
        return NKikimr::TConclusionStatus::Fail("Parse serializer in TOlapColumnFamily: Unknow ClassName");
    }
    return Serializer;
}

bool TOlapColumnFamlilyDiff::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamilyDiff& diffColumnFamily, IErrorCollector& errors) {
    if (!diffColumnFamily.HasName()) {
        errors.AddError("Column family: empty field name");
        return false;
    }
    if (!diffColumnFamily.HasSerializer()) {
        errors.AddError("Column family: empty field serializer");
        return false;
    }

    Name = diffColumnFamily.GetName();
    auto resultParseSerializer = ParseSerializer(diffColumnFamily.GetSerializer());
    if (resultParseSerializer.IsFail()) {
        errors.AddError(resultParseSerializer.GetErrorMessage());
        return false;
    }
    return true;
}

bool TOlapColumnFamlilyAdd::ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamily& columnFamily, IErrorCollector& errors) {
    if (!columnFamily.HasName()) {
        errors.AddError("Column family: empty field Name");
        return false;
    }

    if (!columnFamily.HasSerializer()) {
        errors.AddError("Column family: empty field Serializer");
        return false;
    }

    Name = columnFamily.GetName();
    auto resultParseSerializer = ParseSerializer(columnFamily.GetSerializer());
    if (resultParseSerializer.IsFail()) {
        errors.AddError(resultParseSerializer.GetErrorMessage());
        return false;
    }
    SerializerContainer = *resultParseSerializer;
    return true;
}

void TOlapColumnFamlilyAdd::ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnFamily& columnFamily) {
    Name = columnFamily.GetName();
    auto resultParseSerializer = ParseSerializer(columnFamily.GetSerializer());
    Y_ABORT_UNLESS(!resultParseSerializer.IsFail());
    SerializerContainer = *resultParseSerializer;
}

void TOlapColumnFamlilyAdd::Serialize(NKikimrSchemeOp::TOlapColumnFamily& columnFamily) const {
    columnFamily.SetName(Name);
    auto serializer = columnFamily.MutableSerializer();
    SerializerContainer->SerializeToProto(*serializer);
    serializer->SetClassName(SerializerContainer->GetClassName());
}

bool TOlapColumnFamlilyAdd::ApplyDiff(const TOlapColumnFamlilyDiff& diffColumnFamily, IErrorCollector& errors) {
    Y_ABORT_UNLESS(GetName() == diffColumnFamily.GetName());
    if (!diffColumnFamily.GetSerializerContainer()) {
        errors.AddError("Column family: empty field Serializer");
        return false;
    }
    SerializerContainer = diffColumnFamily.GetSerializerContainer();
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
