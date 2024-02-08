#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/streams/brotli/brotli.h>
#include <library/cpp/streams/bzip2/bzip2.h>
#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/streams/lzma/lzma.h>
#include <library/cpp/streams/xz/decompress.h>

#include <util/stream/mem.h>
#include <util/stream/zlib.h>

#include <contrib/libs/snappy/snappy.h>

using namespace NYql::NUdf;

namespace NCompress {
    SIMPLE_UDF(TGzip, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TZLibCompress compress(&output, ZLib::GZip, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TZlib, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TZLibCompress compress(&output, ZLib::ZLib, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TBrotli, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TBrotliCompress compress(&output, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TLzma, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TLzmaCompress compress(&output, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TBZip2, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TBZipCompress compress(&output, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TSnappy, char*(TAutoMap<char*>)) {
        TString result;
        const TStringRef& input = args[0].AsStringRef();
        snappy::Compress(input.Data(), input.Size(), &result);
        return valueBuilder->NewString(result);
    }

    SIMPLE_UDF(TZstd, char*(TAutoMap<char*>, ui8)) {
        TString result;
        TStringOutput output(result);
        TZstdCompress compress(&output, args[1].Get<ui8>());
        compress.Write(args[0].AsStringRef());
        compress.Finish();
        return valueBuilder->NewString(result);
    }
}

namespace NDecompress {
    SIMPLE_UDF(TGzip, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZLibDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TZlib, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZLibDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TBrotli, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TBrotliDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TLzma, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TLzmaDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TBZip2, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TBZipDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TSnappy, char*(TAutoMap<char*>)) {
        TString result;
        const auto& value = args->AsStringRef();
        if (snappy::Uncompress(value.Data(), value.Size(), &result)) {
            return valueBuilder->NewString(result);
        }

        ythrow yexception() << "failed to decompress message with snappy";
     }

    SIMPLE_UDF(TZstd, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZstdDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }

    SIMPLE_UDF(TXz, char*(TAutoMap<char*>)) {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TXzDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    }
}

namespace NTryDecompress {
    SIMPLE_UDF(TGzip, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZLibDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TZlib, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZLibDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TBrotli, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TBrotliDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TLzma, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TLzmaDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TBZip2, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TBZipDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TSnappy, TOptional<char*>(TAutoMap<char*>)) {
        TString result;
        const auto& value = args->AsStringRef();
        if (snappy::Uncompress(value.Data(), value.Size(), &result)) {
            return valueBuilder->NewString(result);
        }
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TZstd, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TZstdDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }

    SIMPLE_UDF(TXz, TOptional<char*>(TAutoMap<char*>)) try {
        const auto& ref = args->AsStringRef();
        TMemoryInput input(ref.Data(), ref.Size());
        TXzDecompress decompress(&input);
        return valueBuilder->NewString(decompress.ReadAll());
    } catch (const std::exception&) {
        return TUnboxedValuePod();
    }
}

#define EXPORTED_COMPRESS_BASE_UDF       TGzip, TZlib, TBrotli, TLzma, TBZip2, TSnappy, TZstd
#define EXPORTED_DECOMPRESS_BASE_UDF     TGzip, TZlib, TBrotli, TLzma, TBZip2, TSnappy, TZstd, TXz
#define EXPORTED_TRY_DECOMPRESS_BASE_UDF TGzip, TZlib, TBrotli, TLzma, TBZip2, TSnappy, TZstd, TXz
