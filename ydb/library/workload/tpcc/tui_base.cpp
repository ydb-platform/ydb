#include "tui_base.h"

#include "util.h"

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/system/backtrace.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <atomic>
#include <exception>
#include <typeinfo>

namespace NYdb::NTPCC {

namespace {

// if we abnormally terminate, we must try to exit TUI first
// to restore our terminal
TMutex ScreenPtrMutex;
ftxui::ScreenInteractive* ScreenPtr = nullptr;

std::terminate_handler PrevTerminateHandler = nullptr;

void CustomTerminateHandler() noexcept {
    // We hold ScreenPtrMutex during ScreenPtr->Exit(),
    // if anything bad happens either in ScreenPtr->Exit()
    // or in another place during this time, and
    // CustomTerminateHandler() is called again, we want to abort.
    static std::atomic_flag onceFlag;
    if (onceFlag.test_and_set()) {
        // give another one a chance to finish
        Sleep(TDuration::Seconds(5));

        std::abort();
    }

    {
        TGuard guard(ScreenPtrMutex);
        if (ScreenPtr) {
            ScreenPtr->Exit();

            // screen exit is asynchronous, try to wait for some time
            for (size_t i = 0; i < 5; ++i) {
                if (GetGlobalInterruptSource().stop_requested()) {
                    break;
                }
                Sleep(TDuration::Seconds(1));
            }
        }
    }

    std::exception_ptr currentException = std::current_exception();

    int depth = std::uncaught_exceptions();

    TStringStream ss;
    ss << "terminate called; uncaught_exceptions=" << depth << Endl;

    bool stackTracePrinted = false;
    if (currentException) {
        try {
            std::rethrow_exception(currentException);
        } catch (const yexception& ex) {
            ss << "Active exception (yexception): " << typeid(ex).name()
                << " what(): " << ex.what() << Endl;
            const auto* backtrace = ex.BackTrace();
            if (backtrace) {
                stackTracePrinted = true;
                ss << ", backtrace: " << ex.BackTrace()->PrintToString();
            }
        } catch (const std::exception& ex) {
            ss << "Active exception (std::exception): " << typeid(ex).name()
                      << " what(): " << ex.what() << Endl;
        } catch (...) {
            ss << "Active exception (non-std type)" << Endl;
        }
    } else {
        ss << "No active exception (std::terminate called explicitly?)" << Endl;
    }

    if (!stackTracePrinted) {
        ss << "======= terminate() call stack ========\n";
        FormatBackTrace(&ss);
        if (auto backtrace = TBackTrace::FromCurrentException(); backtrace.size() > 0) {
            ss << "======== exception call stack =========\n";
            backtrace.PrintTo(ss);
        }
        ss << "=======================================\n";
    }

    Cerr << ss.Str();

    if (PrevTerminateHandler) {
        PrevTerminateHandler();
    } else {
        std::abort();
    }
}

} // anonymous

//-----------------------------------------------------------------------------

TuiBase::TuiBase()
    : Screen(ftxui::ScreenInteractive::Fullscreen())
{
    {
        TGuard guard(ScreenPtrMutex);
        ScreenPtr = &Screen;
    }

    if (PrevTerminateHandler) {
        // sanity check, should not happen
        throw yexception() << "Terminal handler is already set";
    }

    PrevTerminateHandler = std::set_terminate(CustomTerminateHandler);
}

TuiBase::~TuiBase() {
    {
        TGuard guard(ScreenPtrMutex);
        ScreenPtr = nullptr;
    }

    Screen.Exit();
    if (TuiThread.joinable()) {
        TuiThread.join();
    }

    std::set_terminate(PrevTerminateHandler);
}

void TuiBase::StartLoop() {
    TuiThread = std::thread([&] {
        Screen.Loop(BuildComponent());
        // ftxui catches signals and breaks the loop above, but
        // we have to let know the rest of app
        GetGlobalInterruptSource().request_stop();
    });
}

} // namespace NYdb::NTPCC
