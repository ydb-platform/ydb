#include <ydb/library/actors/core/coro_stack_pool.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/system/context.h>
#include <util/system/error.h>

#include <Windows.h>

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <vector>

namespace {

constexpr ui32 StackSize = 64 * 1024;
constexpr const char* ChildEnvName = "YDB_CORO_STACK_POOL_MPROTECT_CHILD";

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

const char* GetChildSpec(ETouchRange range, ETouchMode mode) noexcept {
    if (range == ETouchRange::InBounds) {
        return mode == ETouchMode::Read ? "in-bounds-read" : "in-bounds-write";
    }

    return mode == ETouchMode::Read ? "guard-page-read" : "guard-page-write";
}

bool TryParseChildSpec(const char* spec, ETouchRange& range, ETouchMode& mode) noexcept {
    if (std::strcmp(spec, "in-bounds-read") == 0) {
        range = ETouchRange::InBounds;
        mode = ETouchMode::Read;
        return true;
    }
    if (std::strcmp(spec, "in-bounds-write") == 0) {
        range = ETouchRange::InBounds;
        mode = ETouchMode::Write;
        return true;
    }
    if (std::strcmp(spec, "guard-page-read") == 0) {
        range = ETouchRange::GuardPage;
        mode = ETouchMode::Read;
        return true;
    }
    if (std::strcmp(spec, "guard-page-write") == 0) {
        range = ETouchRange::GuardPage;
        mode = ETouchMode::Write;
        return true;
    }
    return false;
}

void RunChildCommandIfRequested() {
    const char* spec = std::getenv(ChildEnvName);
    if (!spec) {
        return;
    }

    ETouchRange range;
    ETouchMode mode;
    if (!TryParseChildSpec(spec, range, mode)) {
        ExitProcess(2);
    }

    TouchStackMem(range, mode);
    ExitProcess(0);
}

TString QuoteCommandLineArg(const TString& arg) {
    TString result;
    result += '"';

    size_t backslashes = 0;
    for (char ch : arg) {
        if (ch == '\\') {
            ++backslashes;
            continue;
        }

        if (ch == '"') {
            for (size_t i = 0; i < backslashes * 2 + 1; ++i) {
                result += '\\';
            }
            result += ch;
            backslashes = 0;
            continue;
        }

        for (size_t i = 0; i < backslashes; ++i) {
            result += '\\';
        }
        backslashes = 0;
        result += ch;
    }

    for (size_t i = 0; i < backslashes * 2; ++i) {
        result += '\\';
    }
    result += '"';
    return result;
}

TString GetCurrentExecutablePath() {
    std::vector<char> buffer(MAX_PATH);
    for (;;) {
        const DWORD size = GetModuleFileNameA(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
        UNIT_ASSERT_C(size != 0, "GetModuleFileNameA failed: " << LastSystemErrorText());

        if (size < buffer.size()) {
            return TString(buffer.data(), size);
        }

        buffer.resize(buffer.size() * 2);
    }
}

DWORD RunInChild(ETouchRange range, ETouchMode mode) {
    UNIT_ASSERT_C(SetEnvironmentVariableA(ChildEnvName, GetChildSpec(range, mode)),
        "SetEnvironmentVariableA failed: " << LastSystemErrorText());

    STARTUPINFOA startupInfo = {};
    startupInfo.cb = sizeof(startupInfo);
    PROCESS_INFORMATION processInfo = {};

    TString commandLine = QuoteCommandLineArg(GetCurrentExecutablePath());
    std::vector<char> commandLineBuffer(commandLine.begin(), commandLine.end());
    commandLineBuffer.push_back('\0');

    const BOOL created = CreateProcessA(
        nullptr,
        commandLineBuffer.data(),
        nullptr,
        nullptr,
        FALSE,
        0,
        nullptr,
        nullptr,
        &startupInfo,
        &processInfo);
    const DWORD createError = GetLastError();
    SetEnvironmentVariableA(ChildEnvName, nullptr);

    UNIT_ASSERT_C(created, "CreateProcessA failed: " << LastSystemErrorText(createError));

    CloseHandle(processInfo.hThread);

    const DWORD waitResult = WaitForSingleObject(processInfo.hProcess, INFINITE);
    UNIT_ASSERT_C(waitResult == WAIT_OBJECT_0, "WaitForSingleObject failed: " << LastSystemErrorText());

    DWORD exitCode = 0;
    UNIT_ASSERT_C(GetExitCodeProcess(processInfo.hProcess, &exitCode),
        "GetExitCodeProcess failed: " << LastSystemErrorText());
    CloseHandle(processInfo.hProcess);

    return exitCode;
}

void AssertChildExitedNormally(ETouchRange range, ETouchMode mode) {
    const DWORD exitCode = RunInChild(range, mode);
    UNIT_ASSERT_VALUES_EQUAL(exitCode, 0);
}

void AssertChildWasKilled(ETouchRange range, ETouchMode mode) {
    const DWORD exitCode = RunInChild(range, mode);
    UNIT_ASSERT_VALUES_EQUAL(exitCode, static_cast<DWORD>(EXCEPTION_ACCESS_VIOLATION));
}

} // namespace

Y_UNIT_TEST_SUITE(CoroStackPoolMProtect) {
    Y_UNIT_TEST(InBoundsAccessSurvives) {
        RunChildCommandIfRequested();

        AssertChildExitedNormally(ETouchRange::InBounds, ETouchMode::Read);
        AssertChildExitedNormally(ETouchRange::InBounds, ETouchMode::Write);
    }

    Y_UNIT_TEST(GuardPageAccessKillsChild) {
        RunChildCommandIfRequested();

        AssertChildWasKilled(ETouchRange::GuardPage, ETouchMode::Read);
        AssertChildWasKilled(ETouchRange::GuardPage, ETouchMode::Write);
    }
}
