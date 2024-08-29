#pragma once

#include <util/system/yassert.h>
#include <util/string/builder.h>

#define Y_VERIFY(...) Y_ABORT_UNLESS(__VA_ARGS__)
#define Y_FAIL(...) Y_ABORT(__VA_ARGS__)
#define Y_VERIFY_DEBUG(...) Y_DEBUG_ABORT_UNLESS(__VA_ARGS__)

#define Y_VERIFY_S(expr, msg) Y_VERIFY(expr, "%s", (TStringBuilder() << msg).c_str())
#define Y_FAIL_S(msg) Y_FAIL("%s", (TStringBuilder() << msg).c_str())
#define Y_VERIFY_DEBUG_S(expr, msg) Y_VERIFY_DEBUG(expr, "%s", (TStringBuilder() << msg).c_str())

#define Y_ABORT_S(msg) Y_ABORT("%s", (TStringBuilder() << msg).c_str())
#define Y_DEBUG_ABORT_S(msg) Y_DEBUG_ABORT("%s", (TStringBuilder() << msg).c_str())
