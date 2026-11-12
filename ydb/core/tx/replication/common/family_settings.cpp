#include "family_settings.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/replication.pb.h>

#include <util/stream/output.h>

namespace NKikimr::NReplication {

using TFamily = NKikimrReplication::TSchemaChange::TFamily;

TStringBuf GetColumnFamilyName(const NKikimrSchemeOp::TColumnDescription& column) {
    if (column.GetFamilyName()) {
        return column.GetFamilyName();
    }

    return DefaultFamilyName;
}

TStringBuf GetFamilyName(const NKikimrSchemeOp::TFamilyDescription& family) {
    if (family.HasId() && family.GetId() == 0) {
        return DefaultFamilyName;
    }
    if (!family.HasId() && !family.HasName()) {
        return DefaultFamilyName;
    }

    return family.GetName();
}

NKikimrSchemeOp::EColumnCodec GetColumnCodec(const NKikimrSchemeOp::TFamilyDescription& family) {
    if (family.HasColumnCodec()) {
        return family.GetColumnCodec();
    }

    return family.GetCodec() == 1
        ? NKikimrSchemeOp::ColumnCodecLZ4
        : NKikimrSchemeOp::ColumnCodecPlain;
}

TFamilySettings TFamilySettings::FromProto(const NKikimrSchemeOp::TFamilyDescription* family) {
    if (!family) {
        return {
            .Media = "",
            .Compression = TFamily::COMPRESSION_OFF,
            .CacheMode = TFamily::CACHE_MODE_REGULAR,
            .CompressionLevel = std::nullopt,
        };
    }

    const auto& data = family->GetStorageConfig().GetData();
    // SchemeShard may infer a preferred kind with fallback allowed.
    // Only a strict binding represents the explicit CDC media setting.
    const TStringBuf media = data.GetAllowOtherKinds() ? TStringBuf() : data.GetPreferredPoolKind();
    const auto cacheMode = family->GetColumnCacheMode() == NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory
        ? TFamily::CACHE_MODE_IN_MEMORY
        : TFamily::CACHE_MODE_REGULAR;
    auto compression = TFamily::COMPRESSION_UNSPECIFIED;
    std::optional<ui32> compressionLevel;
    switch (GetColumnCodec(*family)) {
        case NKikimrSchemeOp::ColumnCodecPlain:
            compression = TFamily::COMPRESSION_OFF;
            break;
        case NKikimrSchemeOp::ColumnCodecLZ4:
            compression = TFamily::COMPRESSION_LZ4;
            break;
        case NKikimrSchemeOp::ColumnCodecZSTD:
            compression = TFamily::COMPRESSION_ZSTD;
            compressionLevel = family->HasColumnCodecLevel() ? ui32(family->GetColumnCodecLevel()) : 1u;
            break;
    }
    return {
        .Media = media,
        .Compression = compression,
        .CacheMode = cacheMode,
        .CompressionLevel = compressionLevel,
    };
}

bool TFamilySettings::operator==(const TFamilySettings& other) const {
    return Media == other.Media
        && Compression == other.Compression
        && CacheMode == other.CacheMode
        && CompressionLevel == other.CompressionLevel;
}

void TFamilySettings::Out(IOutputStream& out) const {
    out << "{"
        << " Media: '" << Media << "'"
        << " Compression: " << CompressionToString(Compression)
        << " CacheMode: " << CacheModeToString(CacheMode)
        << " CompressionLevel: " << CompressionLevel.value_or(1)
    << " }";
}

EFamilyCompression CompressionFromString(TStringBuf value) {
    if (value == "off") {
        return TFamily::COMPRESSION_OFF;
    } else if (value == "lz4") {
        return TFamily::COMPRESSION_LZ4;
    } else if (value == "zstd") {
        return TFamily::COMPRESSION_ZSTD;
    } else {
        return TFamily::COMPRESSION_UNSPECIFIED;
    }
}

EFamilyCacheMode CacheModeFromString(TStringBuf value) {
    if (value == "regular") {
        return TFamily::CACHE_MODE_REGULAR;
    } else if (value == "in_memory") {
        return TFamily::CACHE_MODE_IN_MEMORY;
    } else {
        return TFamily::CACHE_MODE_UNSPECIFIED;
    }
}

TStringBuf CompressionToString(EFamilyCompression value) {
    switch (value) {
        case TFamily::COMPRESSION_OFF:
            return "off";
        case TFamily::COMPRESSION_LZ4:
            return "lz4";
        case TFamily::COMPRESSION_ZSTD:
            return "zstd";
        default:
            return {};
    }
}

TStringBuf CacheModeToString(EFamilyCacheMode value) {
    switch (value) {
        case TFamily::CACHE_MODE_REGULAR:
            return "regular";
        case TFamily::CACHE_MODE_IN_MEMORY:
            return "in_memory";
        default:
            return {};
    }
}

bool IsValidCompression(EFamilyCompression value) {
    return value == TFamily::COMPRESSION_OFF
        || value == TFamily::COMPRESSION_LZ4
        || value == TFamily::COMPRESSION_ZSTD;
}

bool IsValidCacheMode(EFamilyCacheMode value) {
    return value == TFamily::CACHE_MODE_REGULAR
        || value == TFamily::CACHE_MODE_IN_MEMORY;
}

}

Y_DECLARE_OUT_SPEC(, ::NKikimr::NReplication::TFamilySettings, out, value) {
    value.Out(out);
}
