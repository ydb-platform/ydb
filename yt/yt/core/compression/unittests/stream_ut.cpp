#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/random.h>

#include <library/cpp/streams/brotli/brotli.h>

namespace NYT::NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TCompressStream>
TString Compress(TString data)
{
    TString compressed;
    TStringOutput output(compressed);
    TCompressStream compressStream(&output, 11);
    compressStream.Write(data.data(), data.size());
    compressStream.Finish();
    output.Finish();
    return compressed;
}

template <typename TDecompressStream>
TString Decompress(TString data)
{
    TStringInput input(data);
    TDecompressStream decompressStream(&input);
    return decompressStream.ReadAll();
}

template <typename TCompressStream, typename TDecompressStream>
void TestCase(const TString& s)
{
    EXPECT_EQ(s, Decompress<TDecompressStream>(Compress<TCompressStream>(s)));
}

TString GenerateRandomString(size_t size)
{
    TRandomGenerator generator(42);
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator.Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

TEST(TBrotliStreamTest, HelloWorld)
{
    TestCase<TBrotliCompress, TBrotliDecompress>("hello world");
}

TEST(TBrotliStreamTest, SeveralStreams)
{
    auto s1 = GenerateRandomString(1 << 15);
    auto s2 = GenerateRandomString(1 << 15);
    auto c1 = Compress<TBrotliCompress>(s1);
    auto c2 = Compress<TBrotliCompress>(s2);
    EXPECT_EQ(s1 + s2, Decompress<TBrotliDecompress>(c1 + c2));
}

TEST(TBrotliStreamTest, IncompleteStream)
{
    TString manyAs(64 * 1024, 'a');
    auto compressed = Compress<TBrotliCompress>(manyAs);
    TString truncated(compressed.Data(), compressed.Size() - 1);
    EXPECT_THROW(Decompress<TBrotliDecompress>(truncated), std::exception);
}

TEST(TBrotliStreamTest, 64KB)
{
    auto manyAs = TString(64 * 1024, 'a');
    TString str("Hello from the Matrix!@#% How are you?}{\n\t\a");
    TestCase<TBrotliCompress, TBrotliDecompress>(manyAs + str + manyAs);
}

TEST(TBrotliStreamTest, 1MB)
{
    TestCase<TBrotliCompress, TBrotliDecompress>(GenerateRandomString(1 * 1024 * 1024));
}

TEST(TBrotliStreamTest, Empty)
{
    TestCase<TBrotliCompress, TBrotliDecompress>("");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCompression
