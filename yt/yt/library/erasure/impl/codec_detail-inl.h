#ifndef CODEC_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include codec_detail.h"
// For the sake of sane code completion.
#include "codec_detail.h"
#endif

namespace NYT::NErasure::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying, ECodec Id, bool Bytewise>
TCodec<TUnderlying, Id, Bytewise>::TCodec()
    : Params_(BuildParams(Underlying_))
{ }

template <class TUnderlying, ECodec Id, bool Bytewise>
const TCodecParams& TCodec<TUnderlying, Id, Bytewise>::GetParams() const
{
    return Params_;
}

template <class TUnderlying, ECodec Id, bool Bytewise>
std::vector<TSharedRef> TCodec<TUnderlying, Id, Bytewise>::Encode(const std::vector<TSharedRef>& blocks) const
{
    return Underlying_.Encode(blocks);
}

template <class TUnderlying, ECodec Id, bool Bytewise>
std::vector<TSharedRef> TCodec<TUnderlying, Id, Bytewise>::Decode(
    const std::vector<TSharedRef>& blocks,
    const TPartIndexList& erasedIndices) const
{
    return Underlying_.Decode(blocks, erasedIndices);
}

template <class TUnderlying, ECodec Id, bool Bytewise>
bool TCodec<TUnderlying, Id, Bytewise>::CanRepair(const TPartIndexList& erasedIndices) const
{
    return Underlying_.CanRepair(erasedIndices);
}

template <class TUnderlying, ECodec Id, bool Bytewise>
bool TCodec<TUnderlying, Id, Bytewise>::CanRepair(const TPartIndexSet& erasedIndices) const
{
    return Underlying_.CanRepair(erasedIndices);
}

template <class TUnderlying, ECodec Id, bool Bytewise>
std::optional<TPartIndexList> TCodec<TUnderlying, Id, Bytewise>::GetRepairIndices(const TPartIndexList& erasedIndices) const
{
    return Underlying_.GetRepairIndices(erasedIndices);
}

template <class TUnderlying, ECodec Id, bool Bytewise>
ECodec TCodec<TUnderlying, Id, Bytewise>::GetId() const
{
    return Id;
}

template <class TUnderlying, ECodec Id, bool Bytewise>
TCodecParams TCodec<TUnderlying, Id, Bytewise>::BuildParams(const TUnderlying& underlying)
{
    return {
        .DataPartCount = underlying.GetDataPartCount(),
        .ParityPartCount = underlying.GetParityPartCount(),
        .TotalPartCount = underlying.GetTotalPartCount(),
        .GuaranteedRepairablePartCount = underlying.GetGuaranteedRepairablePartCount(),
        .WordSize = underlying.GetWordSize(),
        .Bytewise = Bytewise,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure::NDetail
