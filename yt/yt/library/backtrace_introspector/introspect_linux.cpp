#include "introspect.h"

#include "private.h"

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/concurrency/fiber.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/memory/safe_memory_reader.h>

#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <library/cpp/yt/backtrace/cursors/frame_pointer/frame_pointer_cursor.h>

#include <library/cpp/yt/backtrace/cursors/interop/interop.h>

#include <library/cpp/yt/misc/thread_name.h>

#include <util/system/yield.h>

#include <sys/syscall.h>

namespace NYT::NBacktraceIntrospector {

using namespace NConcurrency;
using namespace NTracing;
using namespace NBacktrace;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BacktraceIntrospectorLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TStaticString
{
    TStaticString() = default;

    explicit TStaticString(TStringBuf str)
    {
        Length = std::min(std::ssize(str), std::ssize(Buffer));
        std::copy(str.data(), str.data() + Length, Buffer.data());
    }

    operator TString() const
    {
        return TString(Buffer.data(), static_cast<size_t>(Length));
    }

    std::array<char, 256> Buffer;
    int Length = 0;
};

struct TStaticBacktrace
{
    operator std::vector<const void*>() const
    {
        return std::vector<const void*>(Frames.data(), Frames.data() + FrameCount);
    }

    std::array<const void*, 100> Frames;
    int FrameCount = 0;
};

struct TSignalHandlerContext
{
    TSignalHandlerContext();
    ~TSignalHandlerContext();

    std::atomic<bool> Finished = false;

    TFiberId FiberId = {};
    TTraceId TraceId = {};
    TStaticString TraceLoggingTag;
    TStaticBacktrace Backtrace;
    TThreadName ThreadName = {};

    TSafeMemoryReader* MemoryReader = Singleton<TSafeMemoryReader>();

    void SetFinished()
    {
        Finished.store(true);
    }

    void WaitUntilFinished()
    {
        while (!Finished.load()) {
            ThreadYield();
        }
    }
};

static TSignalHandlerContext* SignalHandlerContext;

TSignalHandlerContext::TSignalHandlerContext()
{
    YT_VERIFY(!SignalHandlerContext);
    SignalHandlerContext = this;
}

TSignalHandlerContext::~TSignalHandlerContext()
{
    YT_VERIFY(SignalHandlerContext == this);
    SignalHandlerContext = nullptr;
}

void SignalHandler(int sig, siginfo_t* /*info*/, void* threadContext)
{
    YT_VERIFY(sig == SIGUSR1);

    SignalHandlerContext->FiberId = GetCurrentFiberId();
    SignalHandlerContext->ThreadName = GetCurrentThreadName();
    if (const auto* traceContext = TryGetCurrentTraceContext()) {
        SignalHandlerContext->TraceId = traceContext->GetTraceId();
        SignalHandlerContext->TraceLoggingTag = TStaticString(traceContext->GetLoggingTag());
    }

    auto cursorContext = FramePointerCursorContextFromUcontext(*static_cast<const ucontext_t*>(threadContext));
    TFramePointerCursor cursor(SignalHandlerContext->MemoryReader, cursorContext);
    while (!cursor.IsFinished() && SignalHandlerContext->Backtrace.FrameCount < std::ssize(SignalHandlerContext->Backtrace.Frames)) {
        SignalHandlerContext->Backtrace.Frames[SignalHandlerContext->Backtrace.FrameCount++] = cursor.GetCurrentIP();
        cursor.MoveNext();
    }

    SignalHandlerContext->SetFinished();
}

} // namespace

std::vector<TThreadIntrospectionInfo> IntrospectThreads()
{
    static std::atomic<bool> IntrospectionLock;

    if (IntrospectionLock.exchange(true)) {
        THROW_ERROR_EXCEPTION("Thread introspection is already in progress");
    }

    auto introspectionLockGuard = Finally([] {
        YT_VERIFY(IntrospectionLock.exchange(false));
    });

    YT_LOG_INFO("Thread introspection started");

    {
        struct sigaction action;
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        ::sigemptyset(&action.sa_mask);
        action.sa_sigaction = SignalHandler;

        if (::sigaction(SIGUSR1, &action, nullptr) != 0) {
            THROW_ERROR_EXCEPTION("Failed to install signal handler")
                << TError::FromSystem();
        }
    }

    std::vector<TThreadIntrospectionInfo> infos;
    for (auto threadId : GetCurrentProcessThreadIds()) {
        if (!IsUserspaceThread(threadId)) {
            YT_LOG_DEBUG("Skipping a non-userspace thread (ThreadId: %v)",
                threadId);
            continue;
        }

        TSignalHandlerContext signalHandlerContext;
        if (::syscall(SYS_tkill, threadId, SIGUSR1) != 0) {
            YT_LOG_DEBUG(TError::FromSystem(), "Failed to signal to thread (ThreadId: %v)",
                threadId);
            continue;
        }

        YT_LOG_DEBUG("Sent signal to thread (ThreadId: %v)",
            threadId);

        signalHandlerContext.WaitUntilFinished();

        YT_LOG_DEBUG("Signal handler finished (ThreadId: %v, FiberId: %x)",
            threadId,
            signalHandlerContext.FiberId);

        infos.push_back(TThreadIntrospectionInfo{
            .ThreadId = threadId,
            .FiberId = signalHandlerContext.FiberId,
            .ThreadName = TString(signalHandlerContext.ThreadName.Buffer.data(), static_cast<size_t>(signalHandlerContext.ThreadName.Length)),
            .TraceId = signalHandlerContext.TraceId,
            .TraceLoggingTag = signalHandlerContext.TraceLoggingTag,
            .Backtrace = signalHandlerContext.Backtrace,
        });
    }

    {
        struct sigaction action;
        action.sa_flags = SA_RESTART;
        ::sigemptyset(&action.sa_mask);
        action.sa_handler = SIG_IGN;

        if (::sigaction(SIGUSR1, &action, nullptr) != 0) {
            THROW_ERROR_EXCEPTION("Failed to de-install signal handler")
                << TError::FromSystem();
        }
    }

    YT_LOG_INFO("Thread introspection completed");

    return infos;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
