#include "dictionary_codec.h"
#include "zstd.h"

namespace NYT::NCompression {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

class TZstdDictionaryCompressionCodec
    : public IDictionaryCompressionCodec
{
public:
    int GetMinDictionarySize() const override
    {
        return ZstdGetMinDictionarySize();
    }

    int GetMaxCompressionLevel() const override
    {
        return ZstdGetMaxCompressionLevel();
    }

    int GetDefaultCompressionLevel() const override
    {
        return ZstdGetDefaultCompressionLevel();
    }

    TErrorOr<TSharedRef> TrainCompressionDictionary(
        i64 dictionarySize,
        const std::vector<TSharedRef>& samples) const override
    {
        return ZstdTrainCompressionDictionary(dictionarySize, samples);
    }

    IDictionaryCompressorPtr CreateDictionaryCompressor(
        const IDigestedCompressionDictionaryPtr& digestedCompressionDictionary) const override
    {
        return ZstdCreateDictionaryCompressor(digestedCompressionDictionary);
    }

    IDictionaryDecompressorPtr CreateDictionaryDecompressor(
        const IDigestedDecompressionDictionaryPtr& digestedDecompressionDictionary) const override
    {
        return ZstdCreateDictionaryDecompressor(digestedDecompressionDictionary);
    }

    IDigestedCompressionDictionaryPtr CreateDigestedCompressionDictionary(
        const TSharedRef& compressionDictionary,
        int compressionLevel) const override
    {
        return ZstdCreateDigestedCompressionDictionary(
            compressionDictionary,
            compressionLevel);
    }

    IDigestedDecompressionDictionaryPtr CreateDigestedDecompressionDictionary(
        const TSharedRef& compressionDictionary) const override
    {
        return ZstdCreateDigestedDecompressionDictionary(compressionDictionary);
    }

    TDictionaryCompressionFrameInfo GetFrameInfo(TRef input) const override
    {
        return ZstdGetFrameInfo(input);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDictionaryCompressionCodec* GetDictionaryCompressionCodec()
{
    static TZstdDictionaryCompressionCodec result;
    return &result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
