#pragma once

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

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

    TArrowOutputFormat(Ydb::Formats::SchemaInclusionMode mode = Ydb::Formats::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_ALWAYS,
        ECompression compression = ECompression::NONE, int32_t level = std::numeric_limits<int32_t>::min())
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
        return TArrowOutputFormat{proto.schema_inclusion_mode(), ECompression(proto.compression().type()), proto.compression().level()};
    }

    ECompression Compression;
    int32_t CompressionLevel;
};

using TOutputFormat = std::variant<TValueOutputFormat, TArrowOutputFormat>;

} // namespace NKikimr::NKqp
