#include "safe_assert.h"

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/backtrace.h>

namespace NYT {

using namespace NConcurrency;
using namespace NCoreDump;

////////////////////////////////////////////////////////////////////////////////

TAssertionFailedException::TAssertionFailedException(
    const TString& expression,
    const TString& stackTrace,
    const std::optional<TString>& corePath)
    : Expression_(expression)
    , StackTrace_(stackTrace)
    , CorePath_(corePath)
{ }

////////////////////////////////////////////////////////////////////////////////

struct TSafeAssertionsFrame
{
    ICoreDumperPtr CoreDumper;
    TAsyncSemaphorePtr CoreSemaphore;
    std::vector<TString> CoreNotes;
};

//! This vector keeps all information about safe frames we have in our stack. The resulting
//! `CoreDumper` and `CoreSemaphore` are taken from the last frame, the resulting `CoreNotes` is
//! a union of `CoreNotes` for all frames.
//! If the vector is empty, safe assertions mode is disabled.
using TSafeAssertionsContext = std::vector<TSafeAssertionsFrame>;

TSafeAssertionsContext& SafeAssertionsContext()
{
    static TFlsSlot<TSafeAssertionsContext> Slot;
    return *Slot;
}

////////////////////////////////////////////////////////////////////////////////

class TSafeAssertionGuard
{
public:
    TSafeAssertionGuard(
        ICoreDumperPtr coreDumper,
        TAsyncSemaphorePtr coreSemaphore,
        std::vector<TString> coreNotes)
    {
        Active_ = static_cast<bool>(coreDumper) &&
            static_cast<bool>(coreSemaphore);
        if (Active_) {
            SafeAssertionsContext().emplace_back(
                TSafeAssertionsFrame{std::move(coreDumper), std::move(coreSemaphore), std::move(coreNotes)});
        }
    }

    ~TSafeAssertionGuard()
    {
        Release();
    }

private:
    bool Active_ = false;

    void Release()
    {
        if (Active_) {
            SafeAssertionsContext().pop_back();
            Active_ = false;
        }
    }
};

std::any CreateSafeAssertionGuard(
    ICoreDumperPtr coreDumper,
    TAsyncSemaphorePtr coreSemaphore,
    std::vector<TString> coreNotes)
{
    return std::make_shared<TSafeAssertionGuard>(std::move(coreDumper), std::move(coreSemaphore), std::move(coreNotes));
}

////////////////////////////////////////////////////////////////////////////////

ICoreDumperPtr GetSafeAssertionsCoreDumper()
{
    return SafeAssertionsContext().back().CoreDumper;
}

TAsyncSemaphorePtr GetSafeAssertionsCoreSemaphore()
{
    return SafeAssertionsContext().back().CoreSemaphore;
}

std::vector<TString> GetSafeAssertionsCoreNotes()
{
    std::vector<TString> coreNotes;
    for (const auto& frame : SafeAssertionsContext()) {
        coreNotes.insert(coreNotes.end(), frame.CoreNotes.begin(), frame.CoreNotes.end());
    }
    return coreNotes;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void MaybeThrowSafeAssertionException(TStringBuf message)
{
    // A meaningful override for a weak symbol located in yt/yt/core/misc/assert.cpp.

    if (SafeAssertionsContext().empty()) {
        return;
    }

    auto semaphore = GetSafeAssertionsCoreSemaphore();
    std::optional<TString> corePath;
    if (auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(semaphore)) {
        try {
            std::vector<TString> coreNotes{"Reason: SafeAssertion"};
            auto contextCoreNotes = GetSafeAssertionsCoreNotes();
            coreNotes.insert(coreNotes.end(), contextCoreNotes.begin(), contextCoreNotes.end());
            auto coreDump = GetSafeAssertionsCoreDumper()->WriteCoreDump(coreNotes, "safe_assertion");
            corePath = coreDump.Path;
            // A tricky way to return slot only after core is written.
            coreDump.WrittenEvent.Subscribe(BIND([_ = std::move(semaphoreGuard)] (const TError&) { }));
        } catch (const std::exception&) {
            // Do nothing.
        }
    }
    TStringBuilder stackTrace;
    DumpBacktrace([&] (TStringBuf str) {
        stackTrace.AppendString(str);
    });
    TString expression(message);
    throw TAssertionFailedException(std::move(expression), stackTrace.Flush(), std::move(corePath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
