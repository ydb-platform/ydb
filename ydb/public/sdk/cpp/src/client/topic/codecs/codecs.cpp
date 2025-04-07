#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/streams/zstd/zstd.h>

#include <util/stream/buffer.h>
#include <util/stream/zlib.h>

namespace NYdb::inline Dev::NTopic {

namespace {

class TZLibToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZLibCompress {
public:
    TZLibToStringCompressor(TBuffer& dst, ZLib::StreamType type, size_t quality)
        : TEmbedPolicy<TBufferOutput>(dst)
        , TZLibCompress(TEmbedPolicy::Ptr(), type, quality)
    {
    }
};

class TZstdToStringCompressor: private TEmbedPolicy<TBufferOutput>, public TZstdCompress {
public:
    TZstdToStringCompressor(TBuffer& dst, int quality)
        : TEmbedPolicy<TBufferOutput>(dst)
        , TZstdCompress(TEmbedPolicy::Ptr(), quality)
    {
    }
};

}

std::string TGzipCodec::Decompress(const std::string& data) const {
    TMemoryInput input(data.data(), data.size());
    TString result;
    TStringOutput resultOutput(result);
    TZLibDecompress inputStreamStorage(&input);
    TransferData(&inputStreamStorage, &resultOutput);
    return result;
}

std::unique_ptr<IOutputStream> TGzipCodec::CreateCoder(TBuffer& result, int quality) const {
    return std::make_unique<TZLibToStringCompressor>(result, ZLib::GZip, quality >= 0 ? quality : 6);
}

std::string TZstdCodec::Decompress(const std::string& data) const {
    TMemoryInput input(data.data(), data.size());
    TString result;
    TStringOutput resultOutput(result);
    TZstdDecompress inputStreamStorage(&input);
    TransferData(&inputStreamStorage, &resultOutput);
    return result;
}

std::unique_ptr<IOutputStream> TZstdCodec::CreateCoder(TBuffer& result, int quality) const {
    return std::make_unique<TZstdToStringCompressor>(result, quality);
}

std::string TUnsupportedCodec::Decompress(const std::string&) const {
    throw yexception() << "use of unsupported codec";
}

std::unique_ptr<IOutputStream> TUnsupportedCodec::CreateCoder(TBuffer&, int) const {
    throw yexception() << "use of unsupported codec";
}

}; // namespace NYdb::NTopic
