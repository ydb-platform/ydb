#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NUdfStore {

//! The body is written to a sibling of the final file and moved into place
//! once complete, so a usable name is one whose sibling fits in a directory
//! entry as well.
inline constexpr TStringBuf UdfTmpFileSuffix = ".tmp";

//! A native UDF name doubles as a filename under the output directory, and it
//! comes from the modules table primary key, i.e. straight from the user. Only
//! names that stay a single entry inside that directory -- the temporary
//! sibling included -- are usable.
bool IsSafeUdfFileName(TStringBuf name);

} // namespace NKikimr::NUdfStore
