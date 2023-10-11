#pragma once

#include "lrc.h"
#include "helpers.h"

#include "isa_erasure.h"

extern "C" {
    #include <contrib/libs/isa-l/include/erasure_code.h>
}

#include <library/cpp/sse/sse.h>

#include <util/generic/array_ref.h>

#include <functional>
#include <optional>
#include <vector>

namespace NErasure {

template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TLrcIsa
    : public TLrcCodecBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>
{
    static_assert(WordSize == 8, "ISA-l erasure codes support computations only in GF(2^8)");
public:
    //! Main blob for storing data.
    using TBlobType = typename TCodecTraits::TBlobType;
    //! Main mutable blob for decoding data.
    using TMutableBlobType = typename TCodecTraits::TMutableBlobType;

    static constexpr ui64 RequiredDataAlignment = alignof(ui64);

    TLrcIsa()
        : TLrcCodecBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>()
    {
        EncodeGFTables_.resize(DataPartCount * ParityPartCount * 32, 0);
        GeneratorMatrix_.resize((DataPartCount + ParityPartCount) * DataPartCount, 0);

        for (int row = 0; row < DataPartCount; ++row) {
            GeneratorMatrix_[row * DataPartCount + row] = 1;
        }
        this->template InitializeGeneratorMatrix<typename decltype(GeneratorMatrix_)::value_type>(
            &GeneratorMatrix_[DataPartCount * DataPartCount],
            std::bind(&gf_mul_erasure, std::placeholders::_1, std::placeholders::_1));

        ec_init_tables(
            DataPartCount,
            ParityPartCount,
            &GeneratorMatrix_.data()[DataPartCount * DataPartCount],
            EncodeGFTables_.data());
    }

    std::vector<TBlobType> Encode(const std::vector<TBlobType>& blocks) const override {
        return ISAErasureEncode<DataPartCount, ParityPartCount, TCodecTraits, TBlobType, TMutableBlobType>(EncodeGFTables_, blocks);
    }

    virtual ~TLrcIsa() = default;

private:
    std::vector<TBlobType> FallbackToCodecDecode(
        const std::vector<TBlobType>& blocks,
        TPartIndexList erasedIndices) const override
    {
        return ISAErasureDecode<DataPartCount, ParityPartCount, TCodecTraits, TBlobType, TMutableBlobType>(
            blocks,
            std::move(erasedIndices),
            this->GetXorGroups(),
            GeneratorMatrix_);
    }

    std::vector<unsigned char> GeneratorMatrix_;
    std::vector<unsigned char> EncodeGFTables_;
};

} // NErasure

