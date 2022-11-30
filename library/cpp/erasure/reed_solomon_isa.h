#pragma once

#include "isa_erasure.h"
#include "reed_solomon.h"

extern "C" {
    #include <contrib/libs/isa-l/include/erasure_code.h>
}

#include <util/generic/array_ref.h>

#include <array>

namespace NErasure {

template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TReedSolomonIsa
    : public TReedSolomonBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>
{
    static_assert(WordSize == 8, "ISA-l erasure codes support computations only in GF(2^8)");
public:
    //! Main blob for storing data.
    using TBlobType = typename TCodecTraits::TBlobType;
    //! Main mutable blob for decoding data.
    using TMutableBlobType = typename TCodecTraits::TMutableBlobType;

    TReedSolomonIsa() {
        EncodeGFTables_.resize(DataPartCount * ParityPartCount * 32, 0);
        GeneratorMatrix_.resize((DataPartCount + ParityPartCount) * DataPartCount, 0);

        gf_gen_rs_matrix(
            GeneratorMatrix_.data(),
            DataPartCount + ParityPartCount,
            DataPartCount);

        ec_init_tables(
            DataPartCount,
            ParityPartCount,
            &GeneratorMatrix_.data()[DataPartCount * DataPartCount],
            EncodeGFTables_.data());
    }

    virtual std::vector<TBlobType> Encode(const std::vector<TBlobType>& blocks) const override {
        return ISAErasureEncode<DataPartCount, ParityPartCount, TCodecTraits, TBlobType, TMutableBlobType>(EncodeGFTables_, blocks);
    }

    virtual std::vector<TBlobType> Decode(
        const std::vector<TBlobType>& blocks,
        const TPartIndexList& erasedIndices) const override
    {
        if (erasedIndices.empty()) {
            return std::vector<TBlobType>();
        }

        return ISAErasureDecode<DataPartCount, ParityPartCount, TCodecTraits, TBlobType, TMutableBlobType>(
            blocks,
            erasedIndices,
            TConstArrayRef<TPartIndexList>(),
            GeneratorMatrix_);
    }

    virtual ~TReedSolomonIsa() = default;

private:
    std::vector<unsigned char> GeneratorMatrix_;
    std::vector<unsigned char> EncodeGFTables_;
};

} // namespace NErasure
