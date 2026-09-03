#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NUdfStore {

//! A native UDF name doubles as a filename under the output directory, and it
//! comes from the modules table primary key, i.e. straight from the user. Only
//! names that stay a single entry inside that directory are usable.
bool IsSafeUdfFileName(TStringBuf name);

} // namespace NKikimr::NUdfStore
