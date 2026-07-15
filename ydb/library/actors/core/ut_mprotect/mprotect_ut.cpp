#include <ydb/library/actors/core/coro_stack_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/context.h>

#include <cerrno>
#include <cstdint>
#include <csignal>
#include <cstring>

#include <sys/wait.h>
#include <unistd.h>

namespace {

constexpr ui32 StackSize = 64 * 1024;

enum class ETouchMode {
    Read,
    Write,
};

enum class ETouchRange {
    InBounds,
    GuardPage,
};

volatile char Sink = 0;

char* ShiftAddress(char* address, std::intptr_t offset) noexcept {
    return reinterpret_cast<char*>(reinterpret_cast<std::intptr_t>(address) + offset);
}

char* GetTouchAddress(NActors::IStackMem& stackMem, ETouchRange range) noexcept {
    return range == ETouchRange::GuardPage ? ShiftAddress(stackMem.Begin(), -1) : stackMem.Begin();
}

void TouchStackMem(ETouchRange range, ETouchMode mode) {
    auto stackMem = NActors::TStackMemPool::GetMemPool(
        NActors::TStackMemPool::TPageBucket::Bytes(StackSize))->Allocate();
    volatile char* address = GetTouchAddress(*stackMem, range);

    switch (mode) {
        case ETouchMode::Read:
            Sink ^= *address;
            break;
        case ETouchMode::Write:
            *address = 1;
            break;
    }
}

void ResetFaultSignalHandlers() noexcept {
    // The unittest runner may install crash handlers; the child must die with the default fault signal.
    (void)std::signal(SIGSEGV, SIG_DFL);
    (void)std::signal(SIGBUS, SIG_DFL);
}

int RunInChild(ETouchRange range, ETouchMode mode) {
    const pid_t pid = fork();
    UNIT_ASSERT_C(pid >= 0, "fork failed: " << std::strerror(errno));

    if (pid == 0) {
        ResetFaultSignalHandlers();
        TouchStackMem(range, mode);
        _exit(0);
    }

    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(pid, &status, 0);
    } while (waited < 0 && errno == EINTR);

    UNIT_ASSERT_C(waited == pid, "waitpid failed: " << std::strerror(errno));
    return status;
}

void AssertChildExitedNormally(ETouchRange range, ETouchMode mode) {
    const int status = RunInChild(range, mode);
    UNIT_ASSERT_C(WIFEXITED(status), "child did not exit normally, status# " << status);
    UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), 0);
}

void AssertChildWasKilled(ETouchRange range, ETouchMode mode) {
    const int status = RunInChild(range, mode);
    UNIT_ASSERT_C(WIFSIGNALED(status), "child was expected to be killed, status# " << status);
    const int signal = WTERMSIG(status);
    UNIT_ASSERT_C(signal == SIGSEGV || signal == SIGBUS, "unexpected child signal# " << signal);
}

} // namespace

Y_UNIT_TEST_SUITE(CoroStackPoolMProtect) {
    Y_UNIT_TEST(InBoundsAccessSurvives) {
        AssertChildExitedNormally(ETouchRange::InBounds, ETouchMode::Read);
        AssertChildExitedNormally(ETouchRange::InBounds, ETouchMode::Write);
    }

    Y_UNIT_TEST(GuardPageAccessKillsChild) {
        AssertChildWasKilled(ETouchRange::GuardPage, ETouchMode::Read);
        AssertChildWasKilled(ETouchRange::GuardPage, ETouchMode::Write);
    }
}
