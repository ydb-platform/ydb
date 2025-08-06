#pragma once

#include <arrow/ipc/options.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <yql/essentials/utils/yql_panic.h>

#include <arrow/util/type_fwd.h>

#include <optional>

namespace NKikimr::NKqp {

struct TArrowFormatSettings {
    struct TCompressionCodec {
        enum class EType {
            UNSPECIFIED = 0,
            NONE = 1,
            ZSTD = 2,
            LZ4_FRAME = 3,
        };

        EType Type = EType::UNSPECIFIED;
        std::optional<i32> Level;
    };

    void ExportToProto(Ydb::Formats::ArrowFormatSettings* proto) const {
        if (CompressionCodec) {
            auto codec = proto->mutable_compression_codec();
            codec->set_type(static_cast<Ydb::Formats::ArrowFormatSettings::CompressionCodec::Type>(CompressionCodec->Type));
            if (CompressionCodec->Level) {
                codec->set_level(*CompressionCodec->Level);
            }
        }
    }

    static TArrowFormatSettings ImportFromProto(const Ydb::Formats::ArrowFormatSettings& proto) {
        auto settings = TArrowFormatSettings{};
        if (proto.has_compression_codec()) {
            auto codec = TCompressionCodec{};
            codec.Type = TCompressionCodec::EType(proto.compression_codec().type());
            if (proto.compression_codec().has_level()) {
                codec.Level = proto.compression_codec().level();
            }
            settings.CompressionCodec = std::move(codec);
        }
        return settings;
    }

    void FillWriteOptions(arrow::ipc::IpcWriteOptions& writeOptions) const {
        if (!CompressionCodec) {
            return;
        }
        auto codec = GetArrowCompressionType(CompressionCodec->Type);
        if (codec != arrow::Compression::UNCOMPRESSED) {
            auto level = CompressionCodec->Level.value_or(arrow::util::Codec::UseDefaultCompressionLevel());
            auto resCodec = arrow::util::Codec::Create(codec, level);
            YQL_ENSURE(resCodec.ok(), "Failed to create codec for arrow format: " << resCodec.status().ToString());
            writeOptions.codec.reset((*resCodec).release());
        }
    }

    static arrow::Compression::type GetArrowCompressionType(TCompressionCodec::EType type) {
        switch (type) {
            case TCompressionCodec::EType::UNSPECIFIED:
            case TCompressionCodec::EType::NONE:
                return arrow::Compression::UNCOMPRESSED;
            case TCompressionCodec::EType::LZ4_FRAME:
                return arrow::Compression::LZ4_FRAME;
            case TCompressionCodec::EType::ZSTD:
                return arrow::Compression::ZSTD;
        }
    }

    std::optional<TCompressionCodec> CompressionCodec;
};

class TResultSetFormatSettings {
public:
    TResultSetFormatSettings(
        ::Ydb::ResultSet::Format format = ::Ydb::ResultSet::FORMAT_UNSPECIFIED,
        ::Ydb::Query::SchemaInclusionMode schemaInclusionMode = ::Ydb::Query::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_UNSPECIFIED,
        std::optional<TArrowFormatSettings> arrowFormatSettings = std::nullopt)
        : Format(format)
        , SchemaInclusionMode(schemaInclusionMode)
        , ArrowFormatSettings(std::move(arrowFormatSettings))
    {
        YQL_ENSURE(IsValueFormat() || IsArrowFormat(), "Unsupported result set format");
    }

    bool IsValueFormat() const {
        return Format == ::Ydb::ResultSet::FORMAT_UNSPECIFIED || Format == ::Ydb::ResultSet::FORMAT_VALUE;
    }

    bool IsArrowFormat() const {
        return Format == ::Ydb::ResultSet::FORMAT_ARROW;
    }

    bool IsSchemaInclusionUnspecified() const {
        return SchemaInclusionMode == ::Ydb::Query::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_UNSPECIFIED;
    }

    bool IsSchemaInclusionAlways() const {
        if (!IsSchemaInclusionUnspecified()) {
            return SchemaInclusionMode == ::Ydb::Query::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_ALWAYS;
        }
        if (IsValueFormat()) {
            return true;
        }
        if (IsArrowFormat()) {
            return false;
        }
        YQL_ENSURE(false, "Unreachable");
        return true;
    }

    bool IsSchemaInclusionFirstOnly() const {
        if (!IsSchemaInclusionUnspecified()) {
            return SchemaInclusionMode == ::Ydb::Query::SchemaInclusionMode::SCHEMA_INCLUSION_MODE_FIRST_ONLY;
        }
        if (IsValueFormat()) {
            return false;
        }
        if (IsArrowFormat()) {
            return true;
        }
        YQL_ENSURE(false, "Unreachable");
        return false;
    }

    const std::optional<TArrowFormatSettings>& GetArrowFormatSettings() const {
        return ArrowFormatSettings;
    }

private:
    ::Ydb::ResultSet::Format Format;
    ::Ydb::Query::SchemaInclusionMode SchemaInclusionMode;
    std::optional<TArrowFormatSettings> ArrowFormatSettings;
};

} // namespace NKikimr::NKqp
