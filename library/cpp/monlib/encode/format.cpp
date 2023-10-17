#include "format.h"

#include <util/string/ascii.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NMonitoring {
    static ECompression CompressionFromHeader(TStringBuf value) {
        if (value.empty()) {
            return ECompression::UNKNOWN;
        }

        for (const auto& it : StringSplitter(value).Split(',').SkipEmpty()) {
            TStringBuf token = StripString(it.Token());

            if (AsciiEqualsIgnoreCase(token, NFormatContentEncoding::IDENTITY)) {
                return ECompression::IDENTITY;
            } else if (AsciiEqualsIgnoreCase(token, NFormatContentEncoding::ZLIB)) {
                return ECompression::ZLIB;
            } else if (AsciiEqualsIgnoreCase(token, NFormatContentEncoding::LZ4)) {
                return ECompression::LZ4;
            } else if (AsciiEqualsIgnoreCase(token, NFormatContentEncoding::ZSTD)) {
                return ECompression::ZSTD;
            }
        }

        return ECompression::UNKNOWN;
    }

    static EFormat FormatFromHttpMedia(TStringBuf value) {
        if (AsciiEqualsIgnoreCase(value, NFormatContenType::SPACK)) {
            return EFormat::SPACK;
        } else if (AsciiEqualsIgnoreCase(value, NFormatContenType::JSON)) {
            return EFormat::JSON;
        } else if (AsciiEqualsIgnoreCase(value, NFormatContenType::PROTOBUF)) {
            return EFormat::PROTOBUF;
        } else if (AsciiEqualsIgnoreCase(value, NFormatContenType::TEXT)) {
            return EFormat::TEXT;
        } else if (AsciiEqualsIgnoreCase(value, NFormatContenType::PROMETHEUS)) {
            return EFormat::PROMETHEUS;
        } else if (AsciiEqualsIgnoreCase(value, NFormatContenType::UNISTAT)) {
            return EFormat::UNISTAT;
        }

        return EFormat::UNKNOWN;
    }

    EFormat FormatFromAcceptHeader(TStringBuf value) {
        EFormat result{EFormat::UNKNOWN};

        for (const auto& it : StringSplitter(value).Split(',').SkipEmpty()) {
            TStringBuf token = StripString(it.Token()).Before(';');

            result = FormatFromHttpMedia(token);
            if (result != EFormat::UNKNOWN) {
                break;
            }
        }

        return result;
    }

    EFormat FormatFromContentType(TStringBuf value) {
        value = value.NextTok(';');

        return FormatFromHttpMedia(value);
    }

    TStringBuf ContentTypeByFormat(EFormat format) {
        switch (format) {
            case EFormat::SPACK:
                return NFormatContenType::SPACK;
            case EFormat::JSON:
                return NFormatContenType::JSON;
            case EFormat::PROTOBUF:
                return NFormatContenType::PROTOBUF;
            case EFormat::TEXT:
                return NFormatContenType::TEXT;
            case EFormat::PROMETHEUS:
                return NFormatContenType::PROMETHEUS;
            case EFormat::UNISTAT:
                return NFormatContenType::UNISTAT;
            case EFormat::UNKNOWN:
                return TStringBuf();
        }

        Y_ABORT(); // for GCC
    }

    ECompression CompressionFromAcceptEncodingHeader(TStringBuf value) {
        return CompressionFromHeader(value);
    }

    ECompression CompressionFromContentEncodingHeader(TStringBuf value) {
        return CompressionFromHeader(value);
    }

    TStringBuf ContentEncodingByCompression(ECompression compression) {
        switch (compression) {
            case ECompression::IDENTITY:
                return NFormatContentEncoding::IDENTITY;
            case ECompression::ZLIB:
                return NFormatContentEncoding::ZLIB;
            case ECompression::LZ4:
                return NFormatContentEncoding::LZ4;
            case ECompression::ZSTD:
                return NFormatContentEncoding::ZSTD;
            case ECompression::UNKNOWN:
                return TStringBuf();
        }

        Y_ABORT(); // for GCC
    }

}

template <>
NMonitoring::EFormat FromStringImpl<NMonitoring::EFormat>(const char* str, size_t len) {
    using NMonitoring::EFormat;
    TStringBuf value(str, len);
    if (value == TStringBuf("SPACK")) {
        return EFormat::SPACK;
    } else if (value == TStringBuf("JSON")) {
        return EFormat::JSON;
    } else if (value == TStringBuf("PROTOBUF")) {
        return EFormat::PROTOBUF;
    } else if (value == TStringBuf("TEXT")) {
        return EFormat::TEXT;
    } else if (value == TStringBuf("PROMETHEUS")) {
        return EFormat::PROMETHEUS;
    } else if (value == TStringBuf("UNISTAT")) {
        return EFormat::UNISTAT;
    } else if (value == TStringBuf("UNKNOWN")) {
        return EFormat::UNKNOWN;
    }
    ythrow yexception() << "unknown format: " << value;
}

template <>
void Out<NMonitoring::EFormat>(IOutputStream& o, NMonitoring::EFormat f) {
    using NMonitoring::EFormat;
    switch (f) {
        case EFormat::SPACK:
            o << TStringBuf("SPACK");
            return;
        case EFormat::JSON:
            o << TStringBuf("JSON");
            return;
        case EFormat::PROTOBUF:
            o << TStringBuf("PROTOBUF");
            return;
        case EFormat::TEXT:
            o << TStringBuf("TEXT");
            return;
        case EFormat::PROMETHEUS:
            o << TStringBuf("PROMETHEUS");
            return;
        case EFormat::UNISTAT:
            o << TStringBuf("UNISTAT");
            return;
        case EFormat::UNKNOWN:
            o << TStringBuf("UNKNOWN");
            return;
    }

    Y_ABORT(); // for GCC
}

template <>
NMonitoring::ECompression FromStringImpl<NMonitoring::ECompression>(const char* str, size_t len) {
    using NMonitoring::ECompression;
    TStringBuf value(str, len);
    if (value == TStringBuf("IDENTITY")) {
        return ECompression::IDENTITY;
    } else if (value == TStringBuf("ZLIB")) {
        return ECompression::ZLIB;
    } else if (value == TStringBuf("LZ4")) {
        return ECompression::LZ4;
    } else if (value == TStringBuf("ZSTD")) {
        return ECompression::ZSTD;
    } else if (value == TStringBuf("UNKNOWN")) {
        return ECompression::UNKNOWN;
    }
    ythrow yexception() << "unknown compression: " << value;
}

template <>
void Out<NMonitoring::ECompression>(IOutputStream& o, NMonitoring::ECompression c) {
    using NMonitoring::ECompression;
    switch (c) {
        case ECompression::IDENTITY:
            o << TStringBuf("IDENTITY");
            return;
        case ECompression::ZLIB:
            o << TStringBuf("ZLIB");
            return;
        case ECompression::LZ4:
            o << TStringBuf("LZ4");
            return;
        case ECompression::ZSTD:
            o << TStringBuf("ZSTD");
            return;
        case ECompression::UNKNOWN:
            o << TStringBuf("UNKNOWN");
            return;
    }

    Y_ABORT(); // for GCC
}
