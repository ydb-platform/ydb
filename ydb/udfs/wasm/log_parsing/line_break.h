#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NLogParsing {

//! Split chunk on '\n', skipping empty lines. Returns false when no records.
bool SplitLineBreak(TStringBuf chunk, TVector<TStringBuf>* records);

} // namespace NLogParsing
