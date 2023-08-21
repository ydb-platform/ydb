#pragma once

#include "helpers.h"

#include <library/cpp/sse/sse.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/array_ref.h>

#include <algorithm>
#include <optional>

namespace NErasure {

template <class TCodecTraits, class TBlobType = typename TCodecTraits::TBlobType>
static inline TBlobType Xor(const std::vector<TBlobType>& refs) {
    using TBufferType = typename TCodecTraits::TBufferType;
    size_t size = refs.front().Size();
    TBufferType result = TCodecTraits::AllocateBuffer(size); // this also fills the buffer with zeros
    for (const TBlobType& ref : refs) {
        const char* data = reinterpret_cast<const char*>(ref.Begin());
        size_t pos = 0;
#ifdef ARCADIA_SSE
        for (; pos + sizeof(__m128i) <= size; pos += sizeof(__m128i)) {
            __m128i* dst = reinterpret_cast<__m128i*>(result.Begin() + pos);
            const __m128i* src = reinterpret_cast<const __m128i*>(data + pos);
            _mm_storeu_si128(dst, _mm_xor_si128(_mm_loadu_si128(src), _mm_loadu_si128(dst)));
        }
#endif
        for (; pos < size; ++pos) {
            *(result.Begin() + pos) ^= data[pos];
        }
    }
    return TCodecTraits::FromBufferToBlob(std::move(result));
}

//! Locally Reconstructable Codes
/*!
 *  See https://www.usenix.org/conference/usenixfederatedconferencesweek/erasure-coding-windows-azure-storage
 *  for more details.
 */
template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TLrcCodecBase
    : public ICodec<typename TCodecTraits::TBlobType>
{
    static_assert(DataPartCount % 2 == 0, "Data part count must be even.");
    static_assert(ParityPartCount == 4, "Now we only support n-2-2 scheme for LRC codec");
    static_assert(1 + DataPartCount / 2 < (1 << (WordSize / 2)), "Data part count should be enough small to construct proper matrix.");
public:
    //! Main blob for storing data.
    using TBlobType = typename TCodecTraits::TBlobType;
    //! Main mutable blob for decoding data.
    using TMutableBlobType = typename TCodecTraits::TMutableBlobType;

    static constexpr ui64 RequiredDataAlignment = alignof(ui64);

    TLrcCodecBase() {
        Groups_[0] = MakeSegment(0, DataPartCount / 2);
        // Xor.
        Groups_[0].push_back(DataPartCount);

        Groups_[1] = MakeSegment(DataPartCount / 2, DataPartCount);
        // Xor.
        Groups_[1].push_back(DataPartCount + 1);

        constexpr int totalPartCount = DataPartCount + ParityPartCount;
        if constexpr (totalPartCount <= BitmaskOptimizationThreshold) {
            CanRepair_.resize(1 << totalPartCount);
            for (int mask = 0; mask < (1 << totalPartCount); ++mask) {
                TPartIndexList erasedIndices;
                for (size_t i = 0; i < totalPartCount; ++i) {
                    if ((mask & (1 << i)) == 0) {
                        erasedIndices.push_back(i);
                    }
                }
                CanRepair_[mask] = CalculateCanRepair(erasedIndices);
            }
        }
    }

    /*! Note that if you want to restore any internal data, blocks offsets must by WordSize * sizeof(long) aligned.
     * Though it is possible to restore unaligned data if no more than one index in each Group is failed. See unittests for this case.
     */
    std::vector<TBlobType> Decode(
        const std::vector<TBlobType>& blocks,
        const TPartIndexList& erasedIndices) const override
    {
        if (erasedIndices.empty()) {
            return std::vector<TBlobType>();
        }

        size_t blockLength = blocks.front().Size();
        for (size_t i = 1; i < blocks.size(); ++i) {
            YT_VERIFY(blocks[i].Size() == blockLength);
        }

        TPartIndexList indices = UniqueSortedIndices(erasedIndices);

        // We can restore one block by xor.
        if (indices.size() == 1) {
            int index = erasedIndices.front();
            for (size_t i = 0; i < 2; ++i) {
                if (Contains(Groups_[i], index)) {
                    return std::vector<TBlobType>(1, Xor<TCodecTraits>(blocks));
                }
            }
        }

        TPartIndexList recoveryIndices = GetRepairIndices(indices).value();
        // We can restore two blocks from different groups using xor.
        if (indices.size() == 2 &&
            indices.back() < DataPartCount + 2 &&
            recoveryIndices.back() < DataPartCount + 2)
        {
            std::vector<TBlobType> result;
            for (int index : indices) {
                for (size_t groupIndex = 0; groupIndex < 2; ++groupIndex) {
                    if (!Contains(Groups_[groupIndex], index)) {
                        continue;
                    }

                    std::vector<TBlobType> correspondingBlocks;
                    for (int pos : Groups_[groupIndex]) {
                        for (size_t i = 0; i < blocks.size(); ++i) {
                            if (recoveryIndices[i] != pos) {
                                continue;
                            }
                            correspondingBlocks.push_back(blocks[i]);
                        }
                    }

                    result.push_back(Xor<TCodecTraits>(correspondingBlocks));
                }
            }
            return result;
        }

        return FallbackToCodecDecode(blocks, std::move(indices));
    }

    bool CanRepair(const TPartIndexList& erasedIndices) const final {
        constexpr int totalPartCount = DataPartCount + ParityPartCount;
        if constexpr (totalPartCount <= BitmaskOptimizationThreshold) {
            int mask = (1 << (totalPartCount)) - 1;
            for (int index : erasedIndices) {
                mask -= (1 << index);
            }
            return CanRepair_[mask];
        } else {
            return CalculateCanRepair(erasedIndices);
        }
    }

    bool CanRepair(const TPartIndexSet& erasedIndicesMask) const final {
        constexpr int totalPartCount = DataPartCount + ParityPartCount;
        if constexpr (totalPartCount <= BitmaskOptimizationThreshold) {
            TPartIndexSet mask = erasedIndicesMask;
            return CanRepair_[mask.flip().to_ulong()];
        } else {
            TPartIndexList erasedIndices;
            for (size_t i = 0; i < erasedIndicesMask.size(); ++i) {
                if (erasedIndicesMask[i]) {
                    erasedIndices.push_back(i);
                }
            }
            return CalculateCanRepair(erasedIndices);
        }
    }

    std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const final {
        if (erasedIndices.empty()) {
            return TPartIndexList();
        }

        TPartIndexList indices = UniqueSortedIndices(erasedIndices);

        if (indices.size() > ParityPartCount) {
            return std::nullopt;
        }

        // One erasure from data or xor blocks.
        if (indices.size() == 1) {
            int index = indices.front();
            for (size_t i = 0; i < 2; ++i) {
                if (Contains(Groups_[i], index)) {
                    return Difference(Groups_[i], index);
                }
            }
        }

        // Null if we have 4 erasures in one group.
        if (indices.size() == ParityPartCount) {
            bool intersectsAny = true;
            for (size_t i = 0; i < 2; ++i) {
                if (Intersection(indices, Groups_[i]).empty()) {
                    intersectsAny = false;
                }
            }
            if (!intersectsAny) {
                return std::nullopt;
            }
        }

        // Calculate coverage of each group.
        int groupCoverage[2] = {};
        for (int index : indices) {
            for (size_t i = 0; i < 2; ++i) {
                if (Contains(Groups_[i], index)) {
                    ++groupCoverage[i];
                }
            }
        }

        // Two erasures, one in each group.
        if (indices.size() == 2 && groupCoverage[0] == 1 && groupCoverage[1] == 1) {
            return Difference(Union(Groups_[0], Groups_[1]), indices);
        }

        // Erasures in only parity blocks.
        if (indices.front() >= DataPartCount) {
            return MakeSegment(0, DataPartCount);
        }

        // Remove unnecessary xor parities.
        TPartIndexList result = Difference(0, DataPartCount + ParityPartCount, indices);
        for (size_t i = 0; i < 2; ++i) {
            if (groupCoverage[i] == 0 && indices.size() <= 3) {
                result = Difference(result, DataPartCount + i);
            }
        }
        return result;
    }

    int GetDataPartCount() const override {
        return DataPartCount;
    }

    int GetParityPartCount() const override {
        return ParityPartCount;
    }

    int GetGuaranteedRepairablePartCount() const override {
        return ParityPartCount - 1;
    }

    int GetWordSize() const override {
        return WordSize * sizeof(long);
    }

    virtual ~TLrcCodecBase() = default;

protected:
    // Indices of data blocks and corresponding xor (we have two xor parities).
    TConstArrayRef<TPartIndexList> GetXorGroups() const {
        return Groups_;
    }

    virtual std::vector<TBlobType> FallbackToCodecDecode(
        const std::vector<TBlobType>& /* blocks */,
        TPartIndexList /* erasedIndices */) const = 0;

    template <typename T>
    void InitializeGeneratorMatrix(T* generatorMatrix, const std::function<T(T)>& GFSquare) {
        for (int row = 0; row < ParityPartCount; ++row) {
            for (int column = 0; column < DataPartCount; ++column) {
                int index = row * DataPartCount + column;

                bool isFirstHalf = column < DataPartCount / 2;
                if (row == 0) generatorMatrix[index] = isFirstHalf ? 1 : 0;
                if (row == 1) generatorMatrix[index] = isFirstHalf ? 0 : 1;

                // Let alpha_i be coefficient of first half and beta_i of the second half.
                // Then matrix is non-singular iff:
                //   a) alpha_i, beta_j != 0
                //   b) alpha_i != beta_j
                //   c) alpha_i + alpha_k != beta_j + beta_l
                // for any i, j, k, l.
                if (row == 2) {
                    int shift = isFirstHalf ? 1 : (1 << (WordSize / 2));
                    int relativeColumn = isFirstHalf ? column : (column - (DataPartCount / 2));
                    generatorMatrix[index] = shift * (1 + relativeColumn);
                }

                // The last row is the square of the row before last.
                if (row == 3) {
                    auto prev = generatorMatrix[index - DataPartCount];
                    generatorMatrix[index] = GFSquare(prev);
                }
            }
        }
    }

private:
    bool CalculateCanRepair(const TPartIndexList& erasedIndices) const {
        TPartIndexList indices = UniqueSortedIndices(erasedIndices);
        if (indices.size() > ParityPartCount) {
            return false;
        }

        if (indices.size() == 1) {
            int index = indices.front();
            for (size_t i = 0; i < 2; ++i) {
                if (Contains(Groups_[i], index)) {
                    return true;
                }
            }
        }

        // If 4 indices miss in one block we cannot recover.
        if (indices.size() == ParityPartCount) {
            for (size_t i = 0; i < 2; ++i) {
                if (Intersection(indices, Groups_[i]).empty()) {
                    return false;
                }
            }
        }

        return true;
    }

    TPartIndexList Groups_[2];
    std::vector<bool> CanRepair_;
};

} // namespace NErasure
