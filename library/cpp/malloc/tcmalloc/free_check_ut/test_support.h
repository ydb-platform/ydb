#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/tcmalloc/tcmalloc/generation_allocator.h>
#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <sys/resource.h>
#include <cstdlib>
#include <cstring>
#include <new>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

extern "C" void* TCMallocInternalMalloc(size_t);
extern "C" void TCMallocInternalFree(void*) noexcept;
extern "C" void TCMallocInternalFreeSized(void*, size_t);
extern "C" void* TCMallocInternalRealloc(void*, size_t);
extern "C" void* TCMallocInternalCalloc(size_t, size_t);
extern "C" void* TCMallocInternalMemalign(size_t, size_t);
extern "C" void MallocExtension_EnableForkSupport();
extern "C" size_t nallocx(size_t, int) noexcept;

namespace NGenerationTest {
using Allocator = tcmalloc::tcmalloc_internal::GenerationAllocator;
inline uint64_t FakeNow = 0;
inline uint64_t Clock() { return FakeNow; }
inline void* Backing(size_t size, size_t alignment) {
    void* mapping = mmap(nullptr, size + alignment, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) return nullptr;
    uintptr_t start = reinterpret_cast<uintptr_t>(mapping);
    uintptr_t aligned = (start + alignment - 1) & ~(alignment - 1);
    if (aligned != start) munmap(mapping, aligned - start);
    const size_t tail = start + size + alignment - aligned - size;
    if (tail) munmap(reinterpret_cast<void*>(aligned + size), tail);
    return reinterpret_cast<void*>(aligned);
}
inline Allocator* CreateAllocator(size_t budget = 4 * 1024 * 1024, bool poison = false, uint64_t delay = 100) {
    void* storage = mmap(nullptr, sizeof(Allocator), PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    UNIT_ASSERT(storage != MAP_FAILED);
    auto* allocator = new (storage) Allocator;
    allocator->Init({budget, delay, poison}, Backing, Clock);
    return allocator;
}
struct Diagnostics {
    uintptr_t Pointer = 0;
    uintptr_t Base = 0;
    uintptr_t Expected = 0;
    size_t SlotSize = 0;
    size_t Offset = 0;
    size_t Requested = 0;
    size_t State = 0;
    bool Present = false;
};
inline Diagnostics* ExpectedDiagnostics = nullptr;

inline uintptr_t Address(const void* ptr) {
    return reinterpret_cast<uintptr_t>(ptr);
}
inline uintptr_t SlotBase(const void* ptr, size_t size, size_t alignment) {
    return Address(ptr) & ~(Allocator::Capacity(size, alignment) * 2 - 1);
}
inline void ExpectFields(const void* pointer, uintptr_t base, uintptr_t expected,
                         size_t slotSize, size_t offset, size_t requested, size_t state) {
    *ExpectedDiagnostics = {Address(pointer), base, expected, slotSize, offset, requested, state, true};
}
inline void ExpectAllocated(const void* pointer, const void* current, size_t size,
                            size_t alignment, size_t state = 1) {
    const uintptr_t base = SlotBase(current, size, alignment);
    ExpectFields(pointer, base, Address(current), Allocator::Capacity(size, alignment) * 2,
                 Address(current) - base, size, state);
}
inline void CorruptByte(void* pointer, size_t offset) {
    auto* bytes = static_cast<volatile unsigned char*>(pointer);
    bytes[offset] = bytes[offset] ^ 1;
}

// Isolated allocators retain backing for process lifetime. Expectations are
// shared through mmap so recording them cannot allocate and change slot reuse.
template <typename Scenario>
void Child(Scenario scenario, const char* failure = nullptr) {
    MallocExtension_EnableForkSupport();
    struct SharedExpectation {
        Diagnostics* Value;
        ~SharedExpectation() {
            ExpectedDiagnostics = nullptr;
            munmap(Value, sizeof(Diagnostics));
        }
    } shared{static_cast<Diagnostics*>(mmap(nullptr, sizeof(Diagnostics),
        PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0))};
    UNIT_ASSERT(shared.Value != MAP_FAILED);
    new (shared.Value) Diagnostics;
    ExpectedDiagnostics = shared.Value;
    int pipefd[2];
    UNIT_ASSERT_VALUES_EQUAL(pipe(pipefd), 0);
    const pid_t pid = fork();
    UNIT_ASSERT(pid >= 0);
    if (pid == 0) {
        close(pipefd[0]);
        if (dup2(pipefd[1], STDERR_FILENO) < 0) _exit(98);
        close(pipefd[1]);
        const rlimit coreLimit{0, 0};
        if (setrlimit(RLIMIT_CORE, &coreLimit) != 0) _exit(98);
        FakeNow = 0;
        alarm(30);
        try {
            scenario();
            _exit(0);
        } catch (...) {
            _exit(99);
        }
    }
    close(pipefd[1]);
    TString output;
    char buf[1024];
    for (;;) {
        const ssize_t count = read(pipefd[0], buf, sizeof(buf));
        if (count < 0 && errno == EINTR) continue;
        UNIT_ASSERT_C(count >= 0, "read diagnostic pipe failed");
        if (count == 0) break;
        output.append(buf, count);
    }
    close(pipefd[0]);
    int status = 0;
    pid_t waited;
    do { waited = waitpid(pid, &status, 0); } while (waited < 0 && errno == EINTR);
    UNIT_ASSERT_VALUES_EQUAL(waited, pid);
    if (failure) {
        UNIT_ASSERT_C(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT, output);
        const TString prefix = TString("TCMalloc generation: ") + failure + " ";
        const auto begin = output.find(prefix);
        UNIT_ASSERT_C(begin != TString::npos, output);
        Diagnostics actual;
        char reason[64];
        int consumed = 0;
        const int fields = sscanf(output.c_str() + begin,
            "TCMalloc generation: %63s pointer=0x%" SCNxPTR " base=0x%" SCNxPTR
            " expected=0x%" SCNxPTR " slot_size=%zu offset=%zu requested=%zu state=%zu%n",
            reason, &actual.Pointer, &actual.Base, &actual.Expected,
            &actual.SlotSize, &actual.Offset, &actual.Requested, &actual.State, &consumed);
        UNIT_ASSERT_VALUES_EQUAL_C(fields, 8, output);
        UNIT_ASSERT_C(strcmp(reason, failure) == 0, output);
        UNIT_ASSERT_C(consumed > 0 && begin + consumed < output.size() && output[begin + consumed] == '\n', output);
        if (shared.Value->Present) {
            const auto& expected = *shared.Value;
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Pointer, expected.Pointer, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Base, expected.Base, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Expected, expected.Expected, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.SlotSize, expected.SlotSize, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Offset, expected.Offset, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.Requested, expected.Requested, output);
            UNIT_ASSERT_VALUES_EQUAL_C(actual.State, expected.State, output);
        }
    } else {
        UNIT_ASSERT_C(WIFEXITED(status) && WEXITSTATUS(status) == 0, output);
        UNIT_ASSERT_C(!output.Contains("TCMalloc generation:"), output);
    }
}
inline void* NonFinalGlobalSlot() {
    for (;;) {
        void* ptr = TCMallocInternalMalloc(32);
        UNIT_ASSERT(ptr);
        if ((reinterpret_cast<uintptr_t>(ptr) & 255) < 128) return ptr;
        TCMallocInternalFree(ptr);
    }
}
inline void* Exhaust(Allocator* a, size_t size = 32, size_t alignment = 8) {
    const size_t capacity = Allocator::Capacity(size, alignment);
    for (size_t n = 0; n <= capacity / alignment; ++n) {
        void* ptr = a->Allocate(size, alignment, 0).ptr;
        UNIT_ASSERT(ptr);
        const uintptr_t offset = Address(ptr) - SlotBase(ptr, size, alignment);
        a->Free(ptr);
        if (offset + alignment > capacity) return ptr;
    }
    UNIT_FAIL("generation cycle did not finish");
    return nullptr;
}
}
