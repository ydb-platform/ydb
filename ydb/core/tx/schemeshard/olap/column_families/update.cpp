#include "update.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/formats/arrow/serializer/utils.h>
#include <ydb/core/tx/schemeshard/olap/column_family/column_family.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard {

bool TOlapColumnFamlilyDiff::ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& diffColumnFamily, IErrorCollector& errors) {
    if (!diffColumnFamily.HasName()) {
        errors.AddError("column family: empty field name");
        return false;
    }

    Name = diffColumnFamily.GetName();
    if (diffColumnFamily.HasColumnCodec()) {
        Codec = diffColumnFamily.GetColumnCodec();
    }
    if (diffColumnFamily.HasColumnCodecLevel()) {
        CodecLevel = diffColumnFamily.GetColumnCodecLevel();
    }
    if (diffColumnFamily.HasDataAccessorConstructor()) {
        NArrow::NAccessor::TRequestedConstructorContainer requestedConstructorContainer;
        if (!requestedConstructorContainer.DeserializeFromProto(diffColumnFamily.GetDataAccessorConstructor())) {
            errors.AddError("cannot parse accessor constructor from proto");
            return false;
        }
        AccessorConstructor = requestedConstructorContainer;
    }
    return true;
}

bool TOlapColumnFamlilyAdd::ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& columnFamily, IErrorCollector& errors) {
    TColumnFamily family;
    TConclusionStatus result = family.DeserializeFromProto(columnFamily);
    if (result.IsFail()) {
        errors.AddError(result.GetErrorMessage());
        return false;
    }
    Name = family.GetName();
    if (family.GetColumnCodec().has_value()) {
        auto serializer = family.GetSerializer();
        if (serializer.IsFail()) {
            errors.AddError(serializer.GetErrorMessage());
            return false;
        }
        auto serializerContainer = NArrow::NSerialization::TSerializerContainer::BuildFromProto(serializer.GetResult());
        if (serializerContainer.IsFail()) {
            errors.AddError(serializerContainer.GetErrorMessage());
            return false;
        }
        SerializerContainer = serializerContainer.GetResult();
    }

    if (family.GetAccessorConstructor().has_value() && !AccessorConstructor.DeserializeFromProto(family.GetAccessorConstructor().value())) {
        errors.AddError("cannot parse accessor constructor from proto");
        return false;
    }
    return true;
}

void TOlapColumnFamlilyAdd::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
    TColumnFamily family;
    TConclusionStatus result = family.DeserializeFromProto(columnFamily);
    Y_VERIFY_S(result.IsSuccess(), result.GetErrorMessage());
    Name = family.GetName();
    if (family.GetColumnCodec().has_value()) {
        auto serializer = family.GetSerializer();
        Y_VERIFY_S(serializer.IsSuccess(), serializer.GetErrorMessage());
        Y_VERIFY(SerializerContainer.DeserializeFromProto(serializer.GetResult()));
    }
    if (family.GetAccessorConstructor().has_value()) {
        Y_VERIFY(AccessorConstructor.DeserializeFromProto(family.GetAccessorConstructor().value()));
    }
}

void TOlapColumnFamlilyAdd::Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const {
    columnFamily.SetName(Name);
    if (SerializerContainer.HasObject()) {
        TColumnFamily family;
        TConclusionStatus result = family.SetSerializer(SerializerContainer);
        Y_VERIFY_S(result.IsSuccess(), result.GetErrorMessage());
        Y_VERIFY(family.GetColumnCodec().has_value());
        columnFamily.SetColumnCodec(family.GetColumnCodec().value());
        if (family.GetColumnCodecLevel().has_value()) {
            columnFamily.SetColumnCodecLevel(family.GetColumnCodecLevel().value());
        }
    }
    if (AccessorConstructor.HasObject()) {
        *columnFamily.MutableDataAccessorConstructor() = AccessorConstructor.SerializeToProto();
    }
}

bool TOlapColumnFamlilyAdd::ApplyDiff(const TOlapColumnFamlilyDiff& diffColumnFamily, IErrorCollector& errors) {
    Y_ABORT_UNLESS(GetName() == diffColumnFamily.GetName());
    auto codec = diffColumnFamily.GetCodec();
    auto codecLevel = diffColumnFamily.GetCodecLevel();
    if (codec.has_value() || codecLevel.has_value()) {
        TColumnFamily newColumnFamily;
        if (SerializerContainer.HasObject()) {
            TConclusionStatus result = newColumnFamily.SetSerializer(SerializerContainer);
            if (result.IsFail()) {
                errors.AddError(result.GetErrorMessage());
                return false;
            }
        }
        newColumnFamily.SetName(GetName());
        if (codec.has_value()) {
            newColumnFamily.SetColumnCodec(codec.value());
            newColumnFamily.MutableColumnCodecLevel().reset();
        }
        if (codecLevel.has_value()) {
            newColumnFamily.SetColumnCodecLevel(codecLevel.value());
        }
        auto serializer = newColumnFamily.GetSerializer();
        if (serializer.IsFail()) {
            errors.AddError(serializer.GetErrorMessage());
            return false;
        }
        auto resultBuild = NArrow::NSerialization::TSerializerContainer::BuildFromProto(serializer.GetResult());
        if (resultBuild.IsFail()) {
            errors.AddError(resultBuild.GetErrorMessage());
            return false;
        }
        SerializerContainer = resultBuild.GetResult();
    }
    if (diffColumnFamily.GetAccessorConstructor().HasObject()) {
        AccessorConstructor = diffColumnFamily.GetAccessorConstructor();
    }
    return true;
}

bool TOlapColumnFamiliesUpdate::Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors) {
    TSet<TString> familyNames;
    for (auto&& family : tableSchema.GetColumnFamilies()) {
        auto familyName = family.GetName();
        if (!familyNames.emplace(familyName).second) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "duplicate column family '" << familyName << "'");
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
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "duplicate column family '" << familyName << "'");
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
}  // namespace NKikimr::NSchemeShard
