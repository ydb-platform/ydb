#include "assert.h"

#include "proc.h"

#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/string/raw_formatter.h>

#include <library/cpp/yt/system/handle_eintr.h>

#ifdef _win_
    #include <io.h>
#else
    #include <unistd.h>
#endif

#include <errno.h>

namespace NYT::NDetail {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void MaybeThrowSafeAssertionException(const char* /*message*/, int /*length*/)
{
    // A default implementation has no means of safety.
    // Actual implementation lives in yt/yt/library/safe_assert.
}

void AssertTrapImpl(
    TStringBuf trapType,
    TStringBuf expr,
    TStringBuf file,
    int line,
    TStringBuf function)
{
    TRawFormatter<1024> formatter;
    formatter.AppendString(trapType);
    formatter.AppendString("(");
    formatter.AppendString(expr);
    formatter.AppendString(") at ");
    formatter.AppendString(file);
    formatter.AppendString(":");
    formatter.AppendNumber(line);
    if (function) {
        formatter.AppendString(" in ");
        formatter.AppendString(function);
        formatter.AppendString("\n");
    }

    MaybeThrowSafeAssertionException(formatter.GetData(), formatter.GetBytesWritten());

    HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());

    NLogging::TLogManager::Get()->Shutdown();

    YT_BUILTIN_TRAP();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
