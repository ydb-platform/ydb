#pragma once

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <variant>

namespace NKikimr::NKqp {

struct TOutputFormatBase {
    TOutputFormatBase(Ydb::Formats::SchemaInclusionMode mode = Ydb::Formats::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_ALWAYS)
        : SchemaInclusionMode(mode)
    {}

    Ydb::Formats::SchemaInclusionMode SchemaInclusionMode;
};

struct TValueOutputFormat : public TOutputFormatBase {
    TValueOutputFormat(Ydb::Formats::SchemaInclusionMode mode = Ydb::Formats::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_ALWAYS)
        : TOutputFormatBase(mode)
    {}

    void ExportToProto(Ydb::Formats::ValueOutputFormat* proto) const {
        proto->set_schema_inclusion_mode(SchemaInclusionMode);
    }

    static TValueOutputFormat ImportFromProto(const Ydb::Formats::ValueOutputFormat& proto) {
        return TValueOutputFormat{proto.schema_inclusion_mode()};
    }
};

struct TArrowOutputFormat : public TOutputFormatBase {
    enum class ECompression {
        UNSPECIFIED = 0,
        NONE = 1,
        ZSTD = 2,
        LZ4_FRAME = 3,
    };

    TArrowOutputFormat(ECompression compression = ECompression::NONE, i32 level = std::numeric_limits<i32>::min(),
        Ydb::Formats::SchemaInclusionMode mode = Ydb::Formats::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_ALWAYS)
        : TOutputFormatBase(mode)
        , Compression(compression)
        , CompressionLevel(level)
    {}

    void ExportToProto(Ydb::Formats::ArrowOutputFormat* proto) const {
        proto->mutable_compression()->set_type(static_cast<Ydb::Formats::ArrowOutputFormat::Compression::Type>(Compression));
        proto->mutable_compression()->set_level(CompressionLevel);
        proto->set_schema_inclusion_mode(SchemaInclusionMode);
    }

    static TArrowOutputFormat ImportFromProto(const Ydb::Formats::ArrowOutputFormat& proto) {
        return TArrowOutputFormat{ECompression(proto.compression().type()), proto.compression().level(), proto.schema_inclusion_mode()};
    }

    ECompression Compression;
    i32 CompressionLevel;
};

using TOutputFormat = std::variant<TValueOutputFormat, TArrowOutputFormat>;

} // namespace NKikimr::NKqp
