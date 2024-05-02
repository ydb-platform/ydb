#pragma once

#include <library/cpp/streams/zstd/zstd.h>
#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include "util/stream/str.h"
#include <util/stream/zlib.h>
#include <util/stream/output.h>
#include <util/system/spinlock.h>

#include <unordered_map>

namespace NYdb::NTopic {

enum class ECodec : ui32 {
    RAW = 1,
    GZIP = 2,
    LZOP = 3,
    ZSTD = 4,
    CUSTOM = 10000,
};

inline const TString& GetCodecId(const ECodec codec) {
    static std::unordered_map<ECodec, TString> idByCodec{
        {ECodec::RAW, TString(1, '\0')},
        {ECodec::GZIP, "\1"},
        {ECodec::LZOP, "\2"},
        {ECodec::ZSTD, "\3"}
    };
    Y_ABORT_UNLESS(idByCodec.contains(codec));
    return idByCodec[codec];
}

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

class TCodecMap {
public:
    static TCodecMap& GetTheCodecMap() {
        static TCodecMap instance;
        return instance;
    }

    void Set(ui32 codecId, THolder<ICodec>&& codecImpl) {
        with_lock(Lock) {
            Codecs[codecId] = std::move(codecImpl);
        }
    }

    const ICodec* GetOrThrow(ui32 codecId) const {
        with_lock(Lock) {
            if (!Codecs.contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " not provided";
            }
            return Codecs.at(codecId).Get();
        }
    }


    TCodecMap(const TCodecMap&) = delete;
    TCodecMap(TCodecMap&&) = delete;
    TCodecMap& operator=(const TCodecMap&) = delete;
    TCodecMap& operator=(TCodecMap&&) = delete;

private:
    TCodecMap() = default;

private:
    std::unordered_map<ui32, THolder<ICodec>> Codecs;
    TAdaptiveLock Lock;
};

} // namespace NYdb::NTopic
