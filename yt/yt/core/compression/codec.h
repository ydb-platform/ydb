#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

//! A generic interface for compression/decompression.
struct ICodec
{
    virtual ~ICodec() = default;

    //! Compress a given block.
    virtual TSharedRef Compress(const TSharedRef& block) = 0;

    //! Compress a vector of blocks.
    virtual TSharedRef Compress(const std::vector<TSharedRef>& blocks) = 0;

    //! Decompress a given block.
    virtual TSharedRef Decompress(const TSharedRef& block) = 0;

    //! Decompress a vector of blocks.
    virtual TSharedRef Decompress(const std::vector<TSharedRef>& blocks) = 0;

    //! Returns codec id
    virtual ECodec GetId() const = 0;
};

//! Returns a codec for the registered id.
ICodec* GetCodec(ECodec id);

//! Deprecated codecs information.
const THashSet<ECodec>& GetForbiddenCodecs();
const THashMap<TString, TString>& GetForbiddenCodecNameToAlias();
const std::vector<ECodec>& GetSupportedCodecs();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
