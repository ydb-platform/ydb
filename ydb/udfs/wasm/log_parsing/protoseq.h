#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NLogParsing {

//! Unpack Protoseq frames: [ui32 LE len][payload][syncword 32].
//! Returns true when the whole buffer was consumed (frames may be empty).
//! Returns false on a corrupt stream (same as native non-empty skip).
bool SplitProtoseq(TStringBuf chunk, TVector<TStringBuf>* frames);

} // namespace NLogParsing
