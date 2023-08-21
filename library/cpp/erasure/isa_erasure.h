#pragma once

#include "public.h"

#include "helpers.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/array_ref.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>

#include <vector>

extern "C" {
    #include <contrib/libs/isa-l/include/erasure_code.h>
}

namespace NErasure {

template <class TBlobType>
static inline unsigned char* ConstCast(typename TBlobType::const_iterator blobIter) {
    return const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(blobIter));
}

template <int DataPartCount, int ParityPartCount, class TCodecTraits, class TBlobType = typename TCodecTraits::TBlobType, class TMutableBlobType = typename TCodecTraits::TMutableBlobType>
std::vector<TBlobType> ISAErasureEncode(
    const std::vector<unsigned char>& encodeGFTables,
    const std::vector<TBlobType>& dataBlocks)
{
    YT_VERIFY(dataBlocks.size() == DataPartCount);

    size_t blockLength = dataBlocks.front().Size();
    for (size_t i = 1; i < dataBlocks.size(); ++i) {
        YT_VERIFY(dataBlocks[i].Size() == blockLength);
    }

    std::vector<unsigned char*> dataPointers;
    for (const auto& block : dataBlocks) {
        dataPointers.emplace_back(ConstCast<TBlobType>(block.Begin()));
    }

    std::vector<TMutableBlobType> parities(ParityPartCount);
    std::vector<unsigned char*> parityPointers(ParityPartCount);
    for (size_t i = 0; i < ParityPartCount; ++i) {
        parities[i] = TCodecTraits::AllocateBlob(blockLength);
        parityPointers[i] = ConstCast<TBlobType>(parities[i].Begin());
        memset(parityPointers[i], 0, blockLength);
    }

    ec_encode_data(
        blockLength,
        DataPartCount,
        ParityPartCount,
        const_cast<unsigned char*>(encodeGFTables.data()),
        dataPointers.data(),
        parityPointers.data());

    return std::vector<TBlobType>(parities.begin(), parities.end());
}

template <int DataPartCount, int ParityPartCount, class TCodecTraits, class TBlobType = typename TCodecTraits::TBlobType, class TMutableBlobType = typename TCodecTraits::TMutableBlobType>
std::vector<TBlobType> ISAErasureDecode(
    const std::vector<TBlobType>& dataBlocks,
    const TPartIndexList& erasedIndices,
    TConstArrayRef<TPartIndexList> groups,
    const std::vector<unsigned char>& fullGeneratorMatrix)
{
    YT_VERIFY(dataBlocks.size() >= DataPartCount);
    YT_VERIFY(erasedIndices.size() <= ParityPartCount);

    size_t blockLength = dataBlocks.front().Size();
    for (size_t i = 1; i < dataBlocks.size(); ++i) {
        YT_VERIFY(dataBlocks[i].Size() == blockLength);
    }

    std::vector<unsigned char> partialGeneratorMatrix(DataPartCount * DataPartCount, 0);

    std::vector<unsigned char*> recoveryBlocks;
    for (size_t i = 0; i < DataPartCount; ++i) {
        recoveryBlocks.emplace_back(ConstCast<TBlobType>(dataBlocks[i].Begin()));
    }

    // Groups check is specific for LRC.
    std::vector<int> isGroupHealthy(2, 1);
    for (size_t i = 0; i < 2; ++i) {
        for (const auto& index : erasedIndices) {
            if (!groups.empty() && Contains(groups[0], index)) {
                isGroupHealthy[0] = 0;
            } else if (!groups.empty() && Contains(groups[1], index)) {
                isGroupHealthy[1] = 0;
            }
        }
    }

    // When a group is healthy we cannot use its local parity, thus skip it using gap.
    size_t gap = 0;
    size_t decodeMatrixIndex = 0;
    size_t erasedBlockIndex = 0;
    while (decodeMatrixIndex < DataPartCount) {
        size_t globalIndex = decodeMatrixIndex + erasedBlockIndex + gap;

        if (erasedBlockIndex < erasedIndices.size() &&
            globalIndex == static_cast<size_t>(erasedIndices[erasedBlockIndex]))
        {
            ++erasedBlockIndex;
            continue;
        }

        if (!groups.empty() && globalIndex >= DataPartCount && globalIndex < DataPartCount + 2) {
            if (Contains(groups[0], globalIndex) && isGroupHealthy[0]) {
                ++gap;
                continue;
            }
            if (Contains(groups[1], globalIndex) && isGroupHealthy[1]) {
                ++gap;
                continue;
            }
        }

        memcpy(&partialGeneratorMatrix[decodeMatrixIndex * DataPartCount], &fullGeneratorMatrix[globalIndex * DataPartCount], DataPartCount);
        ++decodeMatrixIndex;
    }

    std::vector<unsigned char> invertedGeneratorMatrix(DataPartCount * DataPartCount, 0);
    int res = gf_invert_matrix(partialGeneratorMatrix.data(), invertedGeneratorMatrix.data(), DataPartCount);
    YT_VERIFY(res == 0);

    std::vector<unsigned char> decodeMatrix(DataPartCount * (DataPartCount + ParityPartCount), 0);

    //! Some magical code from library example.
    for (size_t i = 0; i < erasedIndices.size(); ++i) {
        if (erasedIndices[i] < DataPartCount) {
            memcpy(&decodeMatrix[i * DataPartCount], &invertedGeneratorMatrix[erasedIndices[i] * DataPartCount], DataPartCount);
        } else {
            for (int k = 0; k < DataPartCount; ++k) {
                int val = 0;
                for (int j = 0; j < DataPartCount; ++j) {
                    val ^= gf_mul_erasure(invertedGeneratorMatrix[j * DataPartCount + k], fullGeneratorMatrix[DataPartCount * erasedIndices[i] + j]);
                }

                decodeMatrix[DataPartCount * i + k] = val;
            }
        }
    }

    std::vector<unsigned char> decodeGFTables(DataPartCount * erasedIndices.size() * 32);
    ec_init_tables(DataPartCount, erasedIndices.size(), decodeMatrix.data(), decodeGFTables.data());

    std::vector<TMutableBlobType> recoveredParts;
    std::vector<unsigned char*> recoveredPartsPointers;
    for (size_t i = 0; i < erasedIndices.size(); ++i) {
        recoveredParts.emplace_back(TCodecTraits::AllocateBlob(blockLength));
        recoveredPartsPointers.emplace_back(ConstCast<TBlobType>(recoveredParts.back().Begin()));
        memset(recoveredPartsPointers.back(), 0, blockLength);
    }

    ec_encode_data(
        blockLength,
        DataPartCount,
        erasedIndices.size(),
        decodeGFTables.data(),
        recoveryBlocks.data(),
        recoveredPartsPointers.data());

    return std::vector<TBlobType>(recoveredParts.begin(), recoveredParts.end());
}

} // namespace NErasure

