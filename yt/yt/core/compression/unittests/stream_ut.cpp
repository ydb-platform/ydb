#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/random.h>

#include <library/cpp/streams/brotli/brotli.h>

#include <util/stream/mem.h>

namespace NYT::NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TCompressStream>
std::string Compress(const std::string& data)
{
    // TODO(babenko): migrate to std::string
    TString compressed;
    TStringOutput output(compressed);
    TCompressStream compressStream(&output, 11);
    compressStream.Write(data.data(), data.size());
    compressStream.Finish();
    output.Finish();
    return compressed;
}

template <typename TDecompressStream>
std::string Decompress(const std::string& data)
{
    TMemoryInput input(data);
    TDecompressStream decompressStream(&input);
    return decompressStream.ReadAll();
}

template <typename TCompressStream, typename TDecompressStream>
void TestCase(const std::string& s)
{
    EXPECT_EQ(s, Decompress<TDecompressStream>(Compress<TCompressStream>(s)));
}

std::string GenerateRandomString(size_t size)
{
    TRandomGenerator generator(42);
    std::string result;
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
    std::string manyAs(64 * 1024, 'a');
    auto compressed = Compress<TBrotliCompress>(manyAs);
    std::string truncated(compressed.data(), compressed.size() - 1);
    EXPECT_THROW(Decompress<TBrotliDecompress>(truncated), std::exception);
}

TEST(TBrotliStreamTest, 64KB)
{
    auto manyAs = std::string(64 * 1024, 'a');
    std::string str("Hello from the Matrix!@#% How are you?}{\n\t\a");
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
