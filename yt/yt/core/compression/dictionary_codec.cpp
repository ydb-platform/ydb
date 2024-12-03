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

    i64 EstimateDigestedCompressionDictionarySize(i64 dictionarySize, int compressionLevel) const override
    {
        return ZstdEstimateDigestedCompressionDictionarySize(dictionarySize, compressionLevel);
    }

    i64 EstimateDigestedDecompressionDictionarySize(i64 dictionarySize) const override
    {
        return ZstdEstimateDigestedDecompressionDictionarySize(dictionarySize);
    }

    IDigestedCompressionDictionaryPtr ConstructDigestedCompressionDictionary(
        const TSharedRef& compressionDictionary,
        TSharedMutableRef storage,
        int compressionLevel) const override
    {
        return ZstdConstructDigestedCompressionDictionary(
            compressionDictionary,
            std::move(storage),
            compressionLevel);
    }

    IDigestedDecompressionDictionaryPtr ConstructDigestedDecompressionDictionary(
        const TSharedRef& decompressionDictionary,
        TSharedMutableRef storage) const override
    {
        return ZstdConstructDigestedDecompressionDictionary(
            decompressionDictionary,
            std::move(storage));
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
