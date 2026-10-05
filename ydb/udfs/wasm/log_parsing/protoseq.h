#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NLogParsing {

//! Unpack Protoseq frames: [ui32 LE len][payload][syncword].
class TProtoseqSplitter {
public:
    explicit TProtoseqSplitter(TStringBuf syncWord);

    //! Returns true when the whole buffer was consumed (frames may be empty).
    //! Returns false on a corrupt stream or an empty syncword.
    bool Split(TStringBuf chunk, TVector<TStringBuf>* frames) const;

private:
    bool TryUnpackFrame(TStringBuf& buf, TStringBuf& data) const noexcept;

    TStringBuf SyncWord_;
};

} // namespace NLogParsing
