#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NArrow::NSSA {

bool AsciiContainsIgnoreCaseMemchr(TStringBuf haystack, TStringBuf needle) noexcept;

} // namespace NKikimr::NArrow::NSSA
