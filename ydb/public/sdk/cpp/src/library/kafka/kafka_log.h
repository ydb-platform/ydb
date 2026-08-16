#pragma once

#include <util/generic/string.h>

namespace NKafka {

static constexpr bool DEBUG_ENABLED = false;

TString Hex(const char* begin, const char* end);

}
