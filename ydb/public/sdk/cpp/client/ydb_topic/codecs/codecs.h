#pragma once

#include <library/cpp/streams/zstd/zstd.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include "util/stream/str.h"
#include <util/stream/zlib.h>
#include <util/stream/output.h>

namespace NYdb::NTopic {

class ICodec {
public:
    virtual ~ICodec() = default;
    virtual TString Decompress(const TString& data) const = 0;
    virtual THolder<IOutputStream> CreateCoder(TBuffer& result, int quality) const = 0;
};

class TGzipCodec final : public ICodec {
    class TZLibToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZLibCompress {
    public:
        TZLibToStringCompressor(TBuffer& dst, ZLib::StreamType type, size_t quality)
            : TEmbedPolicy<TBufferOutput>(dst)
            , TZLibCompress(TEmbedPolicy::Ptr(), type, quality)
        {
        }
    };

    TString Decompress(const TString& data) const override {
        TMemoryInput input(data.data(), data.size());
        TString result;
        TStringOutput resultOutput(result);
        TZLibDecompress inputStreamStorage(&input);
        TransferData(&inputStreamStorage, &resultOutput);
        return result;
    }

    THolder<IOutputStream> CreateCoder(TBuffer& result, int quality) const override {
        return MakeHolder<TZLibToStringCompressor>(result, ZLib::GZip, quality >= 0 ? quality : 6);
    }
};

class TZstdCodec final : public ICodec {
    class TZstdToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZstdCompress {
    public:
        TZstdToStringCompressor(TBuffer& dst, int quality)
            : TEmbedPolicy<TBufferOutput>(dst)
            , TZstdCompress(TEmbedPolicy::Ptr(), quality)
        {
        }
    };

    TString Decompress(const TString& data) const override {
        TMemoryInput input(data.data(), data.size());
        TString result;
        TStringOutput resultOutput(result);
        TZstdDecompress inputStreamStorage(&input);
        TransferData(&inputStreamStorage, &resultOutput);
        return result;
    }

    THolder<IOutputStream> CreateCoder(TBuffer& result, int quality) const override {
        return MakeHolder<TZstdToStringCompressor>(result, quality);
    }
};

class TUnsupportedCodec final : public ICodec {
    TString Decompress(const TString&) const override {
        throw yexception() << "use of unsupported codec";
    }

    THolder<IOutputStream> CreateCoder(TBuffer&, int) const override {
        throw yexception() << "use of unsupported codec";
    }
};

} // namespace NYdb::NTopic
