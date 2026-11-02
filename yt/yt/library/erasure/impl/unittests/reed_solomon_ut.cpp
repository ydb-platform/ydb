#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <algorithm>
#include <tuple>

namespace NYT::NErasure {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TReedSolomonTest
    : public ::testing::TestWithParam<std::tuple<ECodec, int, int>>
{ };

TEST_P(TReedSolomonTest, RepairAllErasurePatterns)
{
    const auto& [requestedCodecId, dataPartCount, parityPartCount] = GetParam();
    const auto codecId = GetEffectiveCodecId(requestedCodecId);
    const auto* codec = GetCodecOrThrow(codecId);
    EXPECT_EQ(codec->GetId(), codecId);
    EXPECT_EQ(GetEffectiveCodecId(codecId), codecId);

    const auto& supportedIds = GetSupportedCodecIds();
    EXPECT_NE(std::find(supportedIds.begin(), supportedIds.end(), codecId), supportedIds.end());

    const auto& params = codec->GetParams();
    EXPECT_EQ(params.DataPartCount, dataPartCount);
    EXPECT_EQ(params.ParityPartCount, parityPartCount);
    EXPECT_EQ(params.TotalPartCount, dataPartCount + parityPartCount);
    EXPECT_EQ(params.GuaranteedRepairablePartCount, parityPartCount);
    EXPECT_TRUE(params.Bytewise);

    std::vector<TSharedRef> data;
    for (int part = 0; part < dataPartCount; ++part) {
        auto block = TSharedMutableRef::Allocate(3 * params.WordSize);
        for (int byte = 0; byte < std::ssize(block); ++byte) {
            block.Begin()[byte] = static_cast<char>((part + 1) * (byte + 17) + byte * byte);
        }
        data.push_back(block);
    }

    const auto parity = codec->Encode(data);
    ASSERT_EQ(std::ssize(parity), parityPartCount);
    auto parts = data;
    parts.insert(parts.end(), parity.begin(), parity.end());

    for (ui32 mask = 0; mask < (1U << params.TotalPartCount); ++mask) {
        SCOPED_TRACE(mask);

        TPartIndexList erasedIndices;
        TPartIndexSet erasedSet;
        for (int part = 0; part < params.TotalPartCount; ++part) {
            if (mask & (1U << part)) {
                erasedIndices.push_back(part);
                erasedSet.set(part);
            }
        }

        const bool repairable = std::ssize(erasedIndices) <= parityPartCount;
        EXPECT_EQ(codec->CanRepair(erasedIndices), repairable);
        EXPECT_EQ(codec->CanRepair(erasedSet), repairable);
        const auto repairIndices = codec->GetRepairIndices(erasedIndices);
        ASSERT_EQ(repairIndices.has_value(), repairable);
        if (!repairable) {
            continue;
        }

        std::vector<TSharedRef> repairParts;
        for (const auto& index : *repairIndices) {
            repairParts.push_back(parts[index]);
        }

        const auto repaired = codec->Decode(repairParts, erasedIndices);
        ASSERT_EQ(std::ssize(repaired), std::ssize(erasedIndices));
        for (int index = 0; index < std::ssize(erasedIndices); ++index) {
            EXPECT_EQ(repaired[index].ToStringBuf(), parts[erasedIndices[index]].ToStringBuf());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TReedSolomonTest,
    TReedSolomonTest,
    ::testing::Values(
        std::tuple(ECodec::ReedSolomon_3_3, /*dataPartCount*/ 3, /*parityPartCount*/ 3),
        std::tuple(ECodec::ReedSolomon_4_6, /*dataPartCount*/ 4, /*parityPartCount*/ 6),
        std::tuple(ECodec::ReedSolomon_5_5, /*dataPartCount*/ 5, /*parityPartCount*/ 5),
        std::tuple(ECodec::ReedSolomon_6_3, /*dataPartCount*/ 6, /*parityPartCount*/ 3)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NErasure
