#pragma once

#include "helpers.h"

#include <algorithm>
#include <optional>

namespace NErasure {

template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TReedSolomonBase
    : public ICodec<typename TCodecTraits::TBlobType>
{
public:
    static constexpr ui64 RequiredDataAlignment = alignof(ui64);

    bool CanRepair(const TPartIndexList& erasedIndices) const final {
        return erasedIndices.size() <= ParityPartCount;
    }

    bool CanRepair(const TPartIndexSet& erasedIndices) const final {
        return erasedIndices.count() <= static_cast<size_t>(ParityPartCount);
    }

    std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const final {
        if (erasedIndices.empty()) {
            return TPartIndexList();
        }

        TPartIndexList indices = erasedIndices;
        std::sort(indices.begin(), indices.end());
        indices.erase(std::unique(indices.begin(), indices.end()), indices.end());

        if (indices.size() > static_cast<size_t>(ParityPartCount)) {
            return std::nullopt;
        }

        return Difference(0, DataPartCount + ParityPartCount, indices);
    }

    int GetDataPartCount() const final {
        return DataPartCount;
    }

    int GetParityPartCount() const final {
        return ParityPartCount;
    }

    int GetGuaranteedRepairablePartCount() const final {
        return ParityPartCount;
    }

    int GetWordSize() const final {
        return WordSize * sizeof(long);
    }

    virtual ~TReedSolomonBase() = default;
};

} // namespace NErasure
