#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/compression/codec.h>

#include <contrib/libs/snappy/snappy-sinksource.h>
#include <contrib/libs/snappy/snappy.h>

namespace NYT::NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCodecTest
    : public ::testing::TestWithParam<std::tuple<ECodec, ui64>>
{
protected:
    ICodec* TheCodec()
    {
        return GetCodec(std::get<0>(GetParam()));
    }

    size_t ThePartSize()
    {
        return std::get<1>(GetParam());
    }

    void TestCase(const std::vector<std::string>& pieces)
    {
        std::vector<TSharedRef> refs;
        size_t length = 0;

        for (const auto& piece : pieces) {
            refs.push_back(TSharedRef::FromString(piece));
            length += piece.length();
        }

        auto compressed = TheCodec()->Compress(refs).Split(ThePartSize());
        auto decompressed = TheCodec()->Decompress(compressed);

        ASSERT_EQ(length, decompressed.Size());

        size_t offset = 0;
        for (const auto& piece : pieces) {
            auto actualSharedRef = decompressed.Slice(offset, offset + piece.length());
            auto actualStringBuf = TStringBuf(actualSharedRef.begin(), actualSharedRef.end());
            auto expectedStringBuf = TStringBuf(piece.begin(), piece.end());

            EXPECT_EQ(expectedStringBuf, actualStringBuf);

            offset += piece.length();
        }
    }
};

TEST_P(TCodecTest, HelloWorld)
{
    TestCase({"hello world"});
}

TEST_P(TCodecTest, 64KB)
{
    TestCase({std::string(64 * 1024, 'a')});
}

TEST_P(TCodecTest, 1MB)
{
    TestCase({std::string(1 * 1024 * 1024, 'a')});
}

TEST_P(TCodecTest, VectorHelloWorld)
{
    TestCase({
        "", "", "hello",
        "", "", "world",
        "", "", std::string(10000, 'a'),
        "", "", std::string(10000, 'b'),
        "", ""});
}

TEST_P(TCodecTest, VectorEmptyRefs)
{
    TestCase({"", "", ""});
}

TEST_P(TCodecTest, VectorSingleCharacters)
{
    std::vector<std::string> input(1000, "a");
    TestCase(input);
}

TEST_P(TCodecTest, VectorExpBuffers)
{
    std::vector<std::string> input;
    for (int i = 0; i < 15; ++i) {
        input.emplace_back(1 << i, 'a' + i);
    }
    TestCase(input);
}

INSTANTIATE_TEST_SUITE_P(
    All,
    TCodecTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetSupportedCodecs()),
        ::testing::ValuesIn(std::vector<ui64>({static_cast<ui64>(-1), 1, 1024}))),
    [] (const ::testing::TestParamInfo<std::tuple<ECodec, ui64>>& info) -> std::string {
        return
            "Codec_" +
            std::string(TEnumTraits<ECodec>::ToString(std::get<0>(info.param)).c_str()) + "_PartSize_" +
            ::testing::PrintToString(std::get<1>(info.param));
    });

TEST_F(TCodecTest, QuickLZDeprecated)
{
    EXPECT_THROW_WITH_SUBSTRING(
        GetCodec(ECodec::QuickLz),
        "Unsupported compression codec \"quick_lz\"");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCompression
