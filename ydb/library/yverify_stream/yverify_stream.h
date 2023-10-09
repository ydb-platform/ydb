#pragma once

#include <util/system/yassert.h>
#include <util/string/builder.h>

#define Y_VERIFY_S(expr, msg) Y_ABORT_UNLESS(expr, "%s", (TStringBuilder() << msg).c_str())
#define Y_FAIL_S(msg) Y_FAIL("%s", (TStringBuilder() << msg).c_str())
#define Y_VERIFY_DEBUG_S(expr, msg) Y_VERIFY_DEBUG(expr, "%s", (TStringBuilder() << msg).c_str())
