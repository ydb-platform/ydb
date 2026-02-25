#include "http.h"

#include <zlib.h>

namespace NHttp {

struct TCompressContext::TImpl { // always deflate, make virtual later
    z_stream zs = {};

    virtual ~TImpl() = default;
};

void TCompressContext::InitCompress(TStringBuf encoding) {
    struct TCompressImpl : TCompressContext::TImpl {
        ~TCompressImpl() {
            deflateEnd(&zs);
        }
    };

    const int compressionlevel = Z_BEST_COMPRESSION;
    Impl = std::make_shared<TCompressImpl>();
    if (encoding == "gzip") {
        Impl->zs.zalloc = Z_NULL;
        Impl->zs.zfree = Z_NULL;
        Impl->zs.opaque = Z_NULL;
        if (deflateInit2(&Impl->zs, compressionlevel, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            ALOG_ERROR(THttpConfig::HttpLog, "deflateInit2 failed with level " << compressionlevel);
            Impl.reset();
        }
    } else if (encoding == "deflate") {
        if (deflateInit(&Impl->zs, compressionlevel) != Z_OK) {
            ALOG_ERROR(THttpConfig::HttpLog, "deflateInit failed with level " << compressionlevel);
            Impl.reset();
        }
    } else {
        ALOG_ERROR(THttpConfig::HttpLog, "Unsupported content encoding: " << encoding);
        Impl.reset();
    }
}

void TCompressContext::InitDecompress(TStringBuf encoding) {
    struct TDecompressImpl : TCompressContext::TImpl {
        ~TDecompressImpl() {
            inflateEnd(&zs);
        }
    };

    Impl = std::make_shared<TDecompressImpl>();
    if (encoding == "gzip") {
        Impl->zs.zalloc = Z_NULL;
        Impl->zs.zfree = Z_NULL;
        Impl->zs.opaque = Z_NULL;
        if (inflateInit2(&Impl->zs, 15 + 16) != Z_OK) {
            ALOG_ERROR(THttpConfig::HttpLog, "inflateInit2 failed while decompressing");
            Impl.reset();
        }
    } else if (encoding == "deflate") {
        if (inflateInit(&Impl->zs) != Z_OK) {
            ALOG_ERROR(THttpConfig::HttpLog, "inflateInit failed while decompressing");
            Impl.reset();
        }
    } else {
        ALOG_ERROR(THttpConfig::HttpLog, "Unsupported content encoding: " << encoding);
        Impl.reset();
    }
}

TCompressContext::operator bool() const {
    return Impl != nullptr;
}

void TCompressContext::Clear() {
    Impl.reset();
}

TString TCompressContext::Compress(TStringBuf source, bool finish) {
    if (!Impl) {
        ALOG_ERROR(THttpConfig::HttpLog, "CompressContext is not initialized");
        return {};
    }
    Impl->zs.next_in = (Bytef*)source.data();
    Impl->zs.avail_in = source.size();
    int ret;
    char outbuffer[32768];
    TString result;
    do {
        Impl->zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
        Impl->zs.avail_out = sizeof(outbuffer);
        ret = deflate(&Impl->zs, finish ? Z_FINISH : Z_SYNC_FLUSH);
        result.append(outbuffer, sizeof(outbuffer) - Impl->zs.avail_out);
    } while (ret == Z_OK && (Impl->zs.avail_in > 0 || Impl->zs.avail_out == 0 || finish));
    if (ret != Z_STREAM_END && ret != Z_OK) {
        ALOG_ERROR(THttpConfig::HttpLog, "Exception during zlib compression: (" << ret << ") " << Impl->zs.msg);
        return {};
    }
    if (finish) {
        deflateEnd(&Impl->zs);
    }
    return result;
}

TString TCompressContext::Decompress(TStringBuf source) {
    if (!Impl) {
        ALOG_ERROR(THttpConfig::HttpLog, "CompressContext is not initialized");
        return {};
    }
    Impl->zs.next_in = (Bytef*)source.data();
    Impl->zs.avail_in = source.size();
    int ret;
    char outbuffer[32768];
    TString result;
    do {
        Impl->zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
        Impl->zs.avail_out = sizeof(outbuffer);
        ret = inflate(&Impl->zs, Z_NO_FLUSH);
        result.append(outbuffer, sizeof(outbuffer) - Impl->zs.avail_out);
    } while (ret == Z_OK && (Impl->zs.avail_in > 0 || Impl->zs.avail_out == 0));
    if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
        ALOG_ERROR(THttpConfig::HttpLog, "Exception during zlib decompression: (" << ret << ") " << Impl->zs.msg);
        return {};
    }
    if (ret == Z_STREAM_END) {
        inflateEnd(&Impl->zs);
    }
    return result;
}

TString CompressDeflate(TStringBuf source) {
    TCompressContext context;
    context.InitCompress("deflate");
    return context.Compress(source, true);
}

TString DecompressDeflate(TStringBuf source) {
    TCompressContext context;
    context.InitDecompress("deflate");
    return context.Decompress(source);
}

}
