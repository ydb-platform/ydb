#include "column_family.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/formats/arrow/serializer/utils.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

TConclusionStatus TColumnFamily::DeserializeFromProto(const NKikimrSchemeOp::TFamilyDescription& proto) {
    if (!proto.HasName()) {
        return TConclusionStatus::Fail("field `Name` is empty in proto");
    }

    if (!proto.HasColumnCodec() && proto.HasColumnCodecLevel()) {
        return TConclusionStatus::Fail("field `ColumnCodecLevel` is not empty, but field `ColumnCodec` is empty in proto");
    }

    if (proto.HasId()) {
        Id = proto.GetId();
    }
    Name = proto.GetName();
    if (proto.HasColumnCodec()) {
        ColumnCodec = proto.GetColumnCodec();
    }
    if (proto.HasColumnCodecLevel()) {
        ColumnCodecLevel = proto.GetColumnCodecLevel();
    }
    if (proto.HasDataAccessorConstructor()) {
        AccessorConstructor = proto.GetDataAccessorConstructor();
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TColumnFamily::DeserializeFromProto(const Ydb::Table::ColumnFamily& proto) {
    if (proto.has_data()) {
        return TConclusionStatus::Fail(
            TStringBuilder() << "Field `DATA` is not supported for OLAP tables in column family '" << proto.name() << "'");
    }
    Name = proto.Getname();
    switch (proto.compression()) {
        case Ydb::Table::ColumnFamily::COMPRESSION_UNSPECIFIED:
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_NONE:
            ColumnCodec = NKikimrSchemeOp::ColumnCodecPlain;
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_LZ4:
            ColumnCodec = NKikimrSchemeOp::ColumnCodecLZ4;
            break;
        case Ydb::Table::ColumnFamily::COMPRESSION_ZSTD:
            ColumnCodec = NKikimrSchemeOp::ColumnCodecZSTD;
            break;
        default:
            return TConclusionStatus::Fail(TStringBuilder() << "Unsupported compression value " << (ui32)proto.compression()
                                                            << " in column family '" << proto.name() << "'");
    }
    if (proto.has_compression_level()) {
        ColumnCodecLevel = proto.Getcompression_level();
    }
    return TConclusionStatus::Success();
}

NKikimrSchemeOp::TFamilyDescription TColumnFamily::SerializeToProto() const {
    NKikimrSchemeOp::TFamilyDescription proto;
    SerializeToProto(proto);
    return proto;
}

void TColumnFamily::SerializeToProto(NKikimrSchemeOp::TFamilyDescription& proto) const {
    proto.SetName(Name);
    if (ColumnCodec.has_value()) {
        proto.SetColumnCodec(ColumnCodec.value());
    }
    if (ColumnCodecLevel.has_value()) {
        proto.SetColumnCodecLevel(ColumnCodecLevel.value());
    }
    if (AccessorConstructor.has_value()) {
        *proto.MutableDataAccessorConstructor() = AccessorConstructor.value();
    }
}

TConclusion<NKikimrSchemeOp::TOlapColumn::TSerializer> TColumnFamily::GetSerializer() const {
    NKikimrSchemeOp::TOlapColumn::TSerializer result;
    if (!ColumnCodec.has_value()) {
        return NKikimr::TConclusionStatus::Fail(
            TStringBuilder() << "family `" << Name << "`: can't convert TFamilyDescription to Serializer: field `ColumnCodec` is empty");
    }
    auto codec = NArrow::CompressionFromProto(ColumnCodec.value());
    if (!codec.has_value()) {
        return NKikimr::TConclusionStatus::Fail(TStringBuilder() << "family `" << Name << "`: unknown codec");
    }
    if (ColumnCodecLevel.has_value() && !NArrow::SupportsCompressionLevel(codec.value())) {
        return NKikimr::TConclusionStatus::Fail(TStringBuilder()
                                                << "family `" << Name << "`: codec `" << NArrow::CompressionToString(ColumnCodec.value())
                                                << "` is not support compression level");
    }
    if (ColumnCodecLevel.has_value()) {
        int level = ColumnCodecLevel.value();
        int minLevel = NArrow::MinimumCompressionLevel(codec.value()).value();
        int maxLevel = NArrow::MaximumCompressionLevel(codec.value()).value();
        if (level < minLevel || level > maxLevel) {
            return NKikimr::TConclusionStatus::Fail(TStringBuilder() << "family `" << Name << "`: incorrect level for codec `"
                                                                     << NArrow::CompressionToString(ColumnCodec.value()) << "`. expected: ["
                                                                     << minLevel << ":" << maxLevel << "]");
        }
    }

    result.SetClassName("ARROW_SERIALIZER");
    auto arrowCompression = result.MutableArrowCompression();
    arrowCompression->SetCodec(ColumnCodec.value());
    if (ColumnCodecLevel.has_value()) {
        arrowCompression->SetLevel(ColumnCodecLevel.value());
    }
    return result;
}

TConclusionStatus TColumnFamily::SetSerializer(const NArrow::NSerialization::TSerializerContainer& serializer) {
    if (serializer.GetClassName().empty()) {
        return NKikimr::TConclusionStatus::Fail("convert TSerializerContainer to TFamilyDescription: field `ClassName` is empty");
    }
    if (serializer.GetClassName() == NArrow::NSerialization::TNativeSerializer::GetClassNameStatic()) {
        std::shared_ptr<NArrow::NSerialization::TNativeSerializer> nativeSerializer =
            serializer.GetObjectPtrVerifiedAs<NArrow::NSerialization::TNativeSerializer>();
        ColumnCodec = NKikimr::NArrow::CompressionToProto(nativeSerializer->GetCodecType());
        auto level = nativeSerializer->GetCodecLevel();
        if (level.has_value()) {
            ColumnCodecLevel = level.value();
        }
    } else {
        return NKikimr::TConclusionStatus::Fail("convert TSerializerContainer to TFamilyDescription: Unknown value in field `ClassName`");
    }
    return TConclusionStatus::Success();
}

NKikimrSchemeOp::TFamilyDescription TColumnFamily::GetDefaultColumnFamily() {
    NKikimrSchemeOp::TFamilyDescription defaultFamily;
    defaultFamily.SetName("default");
    defaultFamily.SetId(0);
    return defaultFamily;
}
}
