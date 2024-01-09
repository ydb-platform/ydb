#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <util/random/fast.h>

#include <contrib/libs/zstd/lib/zstd_errors.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompressionContext
{
    IDictionaryCompressorPtr Compressor;
    IDictionaryDecompressorPtr Decompressor;
};

class TDictionaryCompressionTest
    : public ::testing::Test
{
protected:
    TDictionaryCompressionTest()
        : Rng_(100)
    { }

    void SetUp() override
    {
        FixedSequence_ = GenerateRandomSequence(100);
        YT_VERIFY(FixedSequence_.Size() == 100);
        for (int sampleIndex = 0; sampleIndex < 1000; ++sampleIndex) {
            Samples_.push_back(DoGenerateSample());
        }

        CompressionDictionary_ = GetDictionaryCompressionCodec()->TrainCompressionDictionary(
            2 * GetDictionaryCompressionCodec()->GetMinDictionarySize(),
            Samples_);
    }

    const std::vector<TSharedRef>& GetSamples() const
    {
        return Samples_;
    }

    TSharedRef GenerateNewSample()
    {
        return DoGenerateSample();
    }

    const TErrorOr<TSharedRef>& GetCompressionDictionary() const
    {
        return CompressionDictionary_;
    }

    TCompressionContext CreateCompressionContext() const
    {
        auto digestedCompressionDictionary = GetDictionaryCompressionCodec()->CreateDigestedCompressionDictionary(
            GetCompressionDictionary().Value(),
            GetDictionaryCompressionCodec()->GetDefaultCompressionLevel());
        auto compressor = GetDictionaryCompressionCodec()->CreateDictionaryCompressor(digestedCompressionDictionary);

        auto digestedDecompressionDictionary = GetDictionaryCompressionCodec()->CreateDigestedDecompressionDictionary(
            GetCompressionDictionary().Value());
        auto decompressor = GetDictionaryCompressionCodec()->CreateDictionaryDecompressor(digestedDecompressionDictionary);

        // NB: We do not need to store digested dictionaries as they must be referenced within (de)compressor.
        return {
            .Compressor = compressor,
            .Decompressor = decompressor,
        };
    }

private:
    TFastRng64 Rng_;

    TSharedRef FixedSequence_;
    std::vector<TSharedRef> Samples_;
    TErrorOr<TSharedRef> CompressionDictionary_;


    TSharedRef GenerateRandomSequence(int size)
    {
        int alignedSize = AlignUp<int>(size, 8);
        TBlob blob(GetRefCountedTypeCookie<TDefaultBlobTag>(), alignedSize);
        auto* ptr = blob.Begin();
        while (ptr - blob.Begin() < alignedSize) {
            auto value = Rng_.GenRand();
            WritePod(ptr, value);
        }

        blob.Resize(size);
        return TSharedRef::FromBlob(std::move(blob));
    }

    TSharedRef DoGenerateSample()
    {
        int segmentCount = Rng_.GenRand() % 4 + 1;
        std::vector<int> segmentSizes(segmentCount);
        for (int segmentIndex = 0; segmentIndex < segmentCount; ++segmentIndex) {
            segmentSizes[segmentIndex] = std::max<int>(1, Rng_.GenRand() % FixedSequence_.Size());
        }

        TBlob blob(
            GetRefCountedTypeCookie<TDefaultBlobTag>(),
            std::accumulate(segmentSizes.begin(), segmentSizes.end(), 0));
        auto* ptr = blob.Begin();

        for (int segmentIndex = 0; segmentIndex < segmentCount; ++segmentIndex) {
            if (Rng_.GenRand() % 2 == 0) {
                WriteRef(ptr, TRef(FixedSequence_.Begin(), segmentSizes[segmentIndex]));
            } else {
                auto randomSequence = GenerateRandomSequence(segmentSizes[segmentIndex]);
                WriteRef(ptr, randomSequence);
            }
        }
        YT_VERIFY(ptr == blob.End());

        return TSharedRef::FromBlob(std::move(blob));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDictionaryCompressionTest, TestMinDictionaryCompressionSize)
{
    EXPECT_TRUE(GetCompressionDictionary().IsOK());

    auto dictionarySize = GetDictionaryCompressionCodec()->GetMinDictionarySize() >> 1;
    EXPECT_GT(dictionarySize, 0);
    auto dictionaryOrError = GetDictionaryCompressionCodec()->TrainCompressionDictionary(
        dictionarySize,
        GetSamples());
    EXPECT_FALSE(dictionaryOrError.IsOK());

    auto errorAttribute = dictionaryOrError.Attributes().Find<int>("zstd_error_code");
    EXPECT_TRUE(errorAttribute);
    EXPECT_EQ(ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall, errorAttribute);
}

TEST_F(TDictionaryCompressionTest, TestCompressDecompressSimple)
{
    EXPECT_TRUE(GetCompressionDictionary().IsOK());

    TChunkedMemoryPool pool;
    auto context = CreateCompressionContext();

    auto data = GenerateNewSample();
    EXPECT_LT(0, std::ssize(data));
    auto compressedData = context.Compressor->Compress(&pool, data);
    auto* decompressedData = pool.AllocateUnaligned(data.Size());
    context.Decompressor->Decompress(compressedData, TMutableRef(decompressedData, data.Size()));

    EXPECT_EQ(TStringBuf(data.Begin(), data.Size()), TStringBuf(decompressedData, data.Size()));
}

TEST_F(TDictionaryCompressionTest, TestFrameInfo)
{
    EXPECT_TRUE(GetCompressionDictionary().IsOK());

    TChunkedMemoryPool pool;
    auto context = CreateCompressionContext();

    auto data = GenerateNewSample();
    auto compressedData = context.Compressor->Compress(&pool, data);
    // This is fine as long as seed is fixed.
    EXPECT_LT(compressedData.Size(), data.Size());

    auto frameInfo = GetDictionaryCompressionCodec()->GetFrameInfo(compressedData);
    EXPECT_EQ(data.Size(), frameInfo.ContentSize);
}

TEST_F(TDictionaryCompressionTest, TestCompressionRatio)
{
    EXPECT_TRUE(GetCompressionDictionary().IsOK());

    TChunkedMemoryPool pool;
    auto context = CreateCompressionContext();

    int compressedSize = 0;
    int decompressedSize = 0;
    for (int index = 0; index < 1000; ++index) {
        auto data = GenerateNewSample();
        auto compressedData = context.Compressor->Compress(&pool, data);
        auto* decompressedData = pool.AllocateUnaligned(data.Size());
        context.Decompressor->Decompress(compressedData, TMutableRef(decompressedData, data.Size()));
        EXPECT_EQ(TStringBuf(data.Begin(), data.Size()), TStringBuf(decompressedData, data.Size()));

        compressedSize += compressedData.Size();
        decompressedSize += data.Size();
    }

    EXPECT_LT(compressedSize, 0.8 * decompressedSize);
}

TEST_F(TDictionaryCompressionTest, ModifiedFrameHeader)
{
    EXPECT_TRUE(GetCompressionDictionary().IsOK());

    TChunkedMemoryPool pool;
    auto context = CreateCompressionContext();

    auto data = GenerateNewSample();
    auto compressedData = context.Compressor->Compress(&pool, data);

    ZSTD_frameHeader frameHeader;
    auto result = ZSTD_getFrameHeader_advanced(
        &frameHeader,
        compressedData.Begin(),
        compressedData.Size(),
        ZSTD_f_zstd1);
    EXPECT_TRUE(ZSTD_isError(result));
    // Magic number shall not be written to the frame.
    EXPECT_EQ(ZSTD_ErrorCode::ZSTD_error_prefix_unknown, ZSTD_getErrorCode(result));

    result = ZSTD_getFrameHeader_advanced(
        &frameHeader,
        compressedData.Begin(),
        compressedData.Size(),
        ZSTD_f_zstd1_magicless);
    EXPECT_FALSE(ZSTD_isError(result));
    // These are explicitly disabled.
    EXPECT_EQ(0u, frameHeader.dictID);
    EXPECT_EQ(0u, frameHeader.checksumFlag);
    // We do not use other frame types.
    EXPECT_EQ(ZSTD_frame, frameHeader.frameType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCompression
