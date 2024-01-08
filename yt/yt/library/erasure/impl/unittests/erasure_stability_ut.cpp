#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <util/random/random.h>

namespace NYT::NErasure {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TErasureStabilityTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<
        std::tuple<ECodec, std::vector<unsigned char>>>
{
public:
    TBlob GenerateDataBuffer(int wordSize)
    {
        std::vector<unsigned char> data(wordSize);
        for (int i = 0; i < wordSize; ++i) {
            data[i] = RandomNumber<unsigned char>();
        }

        return TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>(), TRef(data.data(), data.size()));
    }
};

TEST_P(TErasureStabilityTest, TErasureStabilityTest)
{
    SetRandomSeed(42);
    const auto& params = GetParam();

    auto* codec = FindCodec(std::get<0>(params));
    if (!codec) {
        return;
    }

    std::vector<TSharedRef> dataParts;
    for (int i = 0; i < codec->GetDataPartCount(); ++i) {
        dataParts.push_back(TSharedRef::FromBlob(GenerateDataBuffer(codec->GetWordSize())));
    }

    auto parities = codec->Encode(dataParts);
    auto expected = std::get<1>(params);

    EXPECT_EQ(expected.size(), parities.size());
    for (int i = 0; i < std::ssize(expected); ++i) {
        // Check only the first element.
        EXPECT_EQ(static_cast<char>(expected[i]), *parities[i].Begin());
    }
}

INSTANTIATE_TEST_SUITE_P(
    TErasureStabilityTest,
    TErasureStabilityTest,
    ::testing::Values(
        std::tuple(
            ECodec::IsaReedSolomon_3_3,
            std::vector<unsigned char>{59, 252, 207}),
        std::tuple(
            ECodec::ReedSolomon_6_3,
            std::vector<unsigned char>{194, 8, 51}),
        std::tuple(
            ECodec::JerasureLrc_12_2_2,
            std::vector<unsigned char>{194, 201, 87, 67}),
        std::tuple(
            ECodec::IsaLrc_12_2_2,
            std::vector<unsigned char>{194, 201, 104, 219}),
        std::tuple(
            ECodec::IsaReedSolomon_6_3,
            std::vector<unsigned char>{194, 60, 234})));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NErasure
