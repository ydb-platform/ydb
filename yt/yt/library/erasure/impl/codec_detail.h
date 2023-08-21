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

template <class TUnderlying>
class TCodec
    : public ICodec
{
public:
    TCodec(ECodec id, bool bytewise)
        : Id_(id)
        , Bytewise_(bytewise)
    { }

    ECodec GetId() const override
    {
        return Id_;
    }

    std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override
    {
        return Underlying_.Encode(blocks);
    }

    std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.Decode(blocks, erasedIndices);
    }

    bool CanRepair(const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.CanRepair(erasedIndices);
    }

    bool CanRepair(const TPartIndexSet& erasedIndices) const override
    {
        return Underlying_.CanRepair(erasedIndices);
    }

    std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.GetRepairIndices(erasedIndices);
    }

    int GetDataPartCount() const override
    {
        return Underlying_.GetDataPartCount();
    }

    int GetParityPartCount() const override
    {
        return Underlying_.GetParityPartCount();
    }

    int GetGuaranteedRepairablePartCount() const override
    {
        return Underlying_.GetGuaranteedRepairablePartCount();
    }

    int GetWordSize() const override
    {
        return Underlying_.GetWordSize();
    }

    bool IsBytewise() const override
    {
        return Bytewise_;
    }

private:
    const ECodec Id_;
    const bool Bytewise_;

    TUnderlying Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure::NDetail
