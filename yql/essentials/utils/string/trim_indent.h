#pragma once

#include <util/generic/fwd.h>

namespace NYql {

// Supports only ' ' as a whitespace and '\n' as a newline.
// Like a https://kotlinlang.org/api/core/kotlin-stdlib/kotlin.text/trim-indent.html.
// Usefull only at unit tests and with raw string literals.
TString TrimIndent(TStringBuf input);

} // namespace NYql
