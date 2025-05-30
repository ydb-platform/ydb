#include "update.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/formats/arrow/serializer/utils.h>

namespace NKikimr::NSchemeShard {

NKikimr::TConclusion<NKikimrSchemeOp::TOlapColumn::TSerializer> ConvertFamilyDescriptionToProtoSerializer(
    const NKikimrSchemeOp::TFamilyDescription& familyDescription) {
    NKikimrSchemeOp::TOlapColumn::TSerializer result;
    if (!familyDescription.HasColumnCodec()) {
        return NKikimr::TConclusionStatus::Fail(TStringBuilder()
                                                << "family `" << familyDescription.GetName()
                                                << "`: can't convert TFamilyDescription to Serializer: field `ColumnCodec` is empty");
    }
    auto codec = NArrow::CompressionFromProto(familyDescription.GetColumnCodec());
    if (!codec.has_value()) {
        return NKikimr::TConclusionStatus::Fail(TStringBuilder() << "family `" << familyDescription.GetName() << "`: unknown codec");
    }
    if (familyDescription.HasColumnCodecLevel() && !NArrow::SupportsCompressionLevel(codec.value())) {
        return NKikimr::TConclusionStatus::Fail(TStringBuilder() << "family `" << familyDescription.GetName() << "`: codec `"
                                                                 << NArrow::CompressionToString(familyDescription.GetColumnCodec())
                                                                 << "` is not support compression level");
    }
    if (familyDescription.HasColumnCodecLevel()) {
        if (!NArrow::SupportsCompressionLevel(codec.value(), familyDescription.GetColumnCodecLevel())) {
            return NKikimr::TConclusionStatus::Fail(TStringBuilder()
                                                    << "family `" << familyDescription.GetName() << "`: incorrect level for codec `"
                                                    << NArrow::CompressionToString(familyDescription.GetColumnCodec()) << "`. expected: ["
                                                    << NArrow::MinimumCompressionLevel(codec.value()).value() << ":"
                                                    << NArrow::MaximumCompressionLevel(codec.value()).value() << "]");
        }
    }

    result.SetClassName("ARROW_SERIALIZER");
    auto arrowCompression = result.MutableArrowCompression();
    arrowCompression->SetCodec(familyDescription.GetColumnCodec());
    if (familyDescription.HasColumnCodecLevel()) {
        arrowCompression->SetLevel(familyDescription.GetColumnCodecLevel());
    }
    return result;
}

NKikimr::TConclusion<NKikimrSchemeOp::TFamilyDescription> ConvertSerializerContainerToFamilyDescription(
    const NArrow::NSerialization::TSerializerContainer& serializer) {
    if (!serializer.HasObject()) {
        return NKikimr::TConclusionStatus::Fail("convert TSerializerContainer to TFamilyDescription: container doesn't have object");
    }
    NKikimrSchemeOp::TFamilyDescription result;
    if (serializer->GetClassName().empty()) {
        return NKikimr::TConclusionStatus::Fail("convert TSerializerContainer to TFamilyDescription: field `ClassName` is empty");
    }
    if (serializer.GetClassName() == NArrow::NSerialization::TNativeSerializer::GetClassNameStatic()) {
        std::shared_ptr<NArrow::NSerialization::TNativeSerializer> nativeSerializer =
            serializer.GetObjectPtrVerifiedAs<NArrow::NSerialization::TNativeSerializer>();
        result.SetColumnCodec(NKikimr::NArrow::CompressionToProto(nativeSerializer->GetCodecType()));
        auto level = nativeSerializer->GetCodecLevel();
        if (level.has_value()) {
            result.SetColumnCodecLevel(level.value());
        }
    } else {
        return NKikimr::TConclusionStatus::Fail("convert TSerializerContainer to TFamilyDescription: Unknown value in field `ClassName`");
    }
    return result;
}

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
    return true;
}

bool TOlapColumnFamlilyAdd::ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& columnFamily, IErrorCollector& errors) {
    if (!columnFamily.HasName()) {
        errors.AddError("column family: empty field Name");
        return false;
    }

    Name = columnFamily.GetName();
    if (columnFamily.HasColumnCodec()) {
        auto serializer = ConvertFamilyDescriptionToProtoSerializer(columnFamily);
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
    return true;
}

void TOlapColumnFamlilyAdd::ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily) {
    Name = columnFamily.GetName();
    if (columnFamily.HasColumnCodec()) {
        auto serializer = ConvertFamilyDescriptionToProtoSerializer(columnFamily);
        Y_VERIFY_S(serializer.IsSuccess(), serializer.GetErrorMessage());
        SerializerContainer = NArrow::NSerialization::TSerializerContainer();
        Y_VERIFY(SerializerContainer.DeserializeFromProto(serializer.GetResult()));
    }
}

void TOlapColumnFamlilyAdd::Serialize(NKikimrSchemeOp::TFamilyDescription& columnFamily) const {
    columnFamily.SetName(Name);
    if (SerializerContainer.HasObject()) {
        auto result = ConvertSerializerContainerToFamilyDescription(SerializerContainer);
        Y_VERIFY_S(result.IsSuccess(), result.GetErrorMessage());
        columnFamily.SetColumnCodec(result->GetColumnCodec());
        if (result->HasColumnCodecLevel()) {
            columnFamily.SetColumnCodecLevel(result->GetColumnCodecLevel());
        }
    }
}

bool TOlapColumnFamlilyAdd::ApplyDiff(const TOlapColumnFamlilyDiff& diffColumnFamily, IErrorCollector& errors) {
    Y_ABORT_UNLESS(GetName() == diffColumnFamily.GetName());
    NKikimrSchemeOp::TFamilyDescription newColumnFamily;
    if (SerializerContainer.HasObject()) {
        auto resultConvert = ConvertSerializerContainerToFamilyDescription(SerializerContainer);
        if (resultConvert.IsFail()) {
            errors.AddError(resultConvert.GetErrorMessage());
            return false;
        }
        newColumnFamily = resultConvert.GetResult();
    }
    newColumnFamily.SetName(GetName());
    auto codec = diffColumnFamily.GetCodec();
    if (codec.has_value()) {
        newColumnFamily.SetColumnCodec(codec.value());
        newColumnFamily.ClearColumnCodecLevel();
    }
    auto codecLevel = diffColumnFamily.GetCodecLevel();
    if (codecLevel.has_value()) {
        newColumnFamily.SetColumnCodecLevel(codecLevel.value());
    }
    auto serializer = ConvertFamilyDescriptionToProtoSerializer(newColumnFamily);
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
}
