#pragma once

#include "public.h"

#include <yt/yt/library/erasure/codec_params.h>

#include <library/cpp/yt/memory/blob.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/erasure/codec.h>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual ~ICodec() = default;

    //! Returns the codec id.
    virtual ECodec GetId() const = 0;

    //! Encodes data blocks, returns parity blocks.
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const = 0;

    //! Decodes (repairs) missing blocks.
    /*!
     *  #erasedIndices must contain the set of erased blocks indices.
     *  #blocks must contain known blocks (in the order specified by #GetRepairIndices).
     *  \returns The repaired blocks.
     */
    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) const = 0;

    //! Given a set of missing block indices, returns |true| if missing blocks can be repaired.
    //! Due to performance reasons the elements of #erasedIndices must unique and sorted.
    virtual bool CanRepair(const TPartIndexList& erasedIndices) const = 0;

    //! Rapid version that works with set instead of list.
    virtual bool CanRepair(const TPartIndexSet& erasedIndices) const = 0;

    //! Given a set of missing block indices, checks if missing blocks can be repaired.
    /*!
     *  \returns
     *  If repair is not possible, returns |std::nullopt|.
     *  Otherwise returns the indices of blocks (both data and parity) to be passed to #Decode
     *  (in this very order). Not all known blocks may be needed for repair.
     */
    virtual std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const = 0;

    //! Returns the codec's static properties.
    virtual const TCodecParams& GetParams() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Finds an erasure codec by id. Returns |nullptr| if codec is not supported.
ICodec* FindCodec(ECodec id);

//! Finds an erasure codec by id. Throws an error if codec is not supported.
ICodec* GetCodecOrThrow(ECodec id);

//! For a given codec id returns the id of most optimal erasure codec
//! that has the same number of data and parity parts.
ECodec GetEffectiveCodecId(ECodec id);

//! Returns the list of supported erasure codecs.
const std::vector<ECodec>& GetSupportedCodecIds();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
