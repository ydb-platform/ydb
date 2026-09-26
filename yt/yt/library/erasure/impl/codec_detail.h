#pragma once

#include "public.h"

#include "codec.h"

namespace NYT::NErasure::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TCodecTraits
{
    using TBlobType = TSharedRef;
    using TMutableBlobType = TSharedMutableRef;
    using TBufferType = TBlob;

    static TMutableBlobType AllocateBlob(size_t size);
    static TBufferType AllocateBuffer(size_t size);
    static TBlobType FromBufferToBlob(TBufferType&& blob);
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying, ECodec Id, bool Bytewise>
class TCodec
    : public ICodec
{
public:
    TCodec();

    const TCodecParams& GetParams() const override;

    std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override;
    std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) const override;

    bool CanRepair(const TPartIndexList& erasedIndices) const override;
    bool CanRepair(const TPartIndexSet& erasedIndices) const override;

    std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const override;

    ECodec GetId() const override;

private:
    //! Declared before #Params_, which is initialized from it.
    TUnderlying Underlying_;

    const TCodecParams Params_;

    static TCodecParams BuildParams(const TUnderlying& underlying);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure::NDetail

#define CODEC_DETAIL_INL_H_
#include "codec_detail-inl.h"
#undef CODEC_DETAIL_INL_H_
