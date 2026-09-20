#pragma once

#include <util/generic/string.h>

namespace NYql::NFmr {

// Read all YSON content from a file that may be in splitted layout (.part.N files).
// If the .attr has "splitted=N", concatenates all N part files in order.
// Otherwise reads the base file directly.
TString ReadFileWithSplittedSupport(const TString& path);

} // namespace NYql::NFmr
