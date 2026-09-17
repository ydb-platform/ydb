#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

extern "C" void* TCMallocInternalMalloc(size_t);
extern "C" void TCMallocInternalFree(void*) noexcept;
extern "C" void TCMallocInternalFreeSized(void*, size_t);
extern "C" void MallocExtension_EnableForkSupport();

namespace {
// Match the diagnostic table's bucket to force eviction deterministically.
size_t Bucket(void* ptr) {
    uintptr_t value = reinterpret_cast<uintptr_t>(ptr) >> 3;
    value ^= value >> 17;
    value *= uintptr_t{0x9e3779b97f4a7c15ULL};
    return (value >> 32) & 4095;
}

void CheckFailure(void (*scenario)(), const char* message) {
    int pipefd[2];
    UNIT_ASSERT_VALUES_EQUAL(pipe(pipefd), 0);
    const pid_t pid = fork();
    UNIT_ASSERT(pid >= 0);
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        tcmalloc::MallocExtension::SetProfileSamplingInterval(0);
        scenario();
        _exit(0);
    }
    close(pipefd[1]);
    TString output;
    char buf[1024];
    ssize_t count;
    while ((count = read(pipefd[0], buf, sizeof(buf))) > 0) {
        output.append(buf, count);
    }
    close(pipefd[0]);
    int status = 0;
    UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, 0), pid);
    UNIT_ASSERT_C(WIFSIGNALED(status), output);
    UNIT_ASSERT_C(output.Contains(message), output);
}

void CorruptAndEvict(size_t offset) {
    void* buckets[4096] = {};
    for (size_t i = 0; i < 4097; ++i) {
        void* ptr = TCMallocInternalMalloc(256);
        const size_t bucket = Bucket(ptr);
        if (void* previous = buckets[bucket]) {
            TCMallocInternalFree(previous);
            auto* bytes = static_cast<volatile unsigned char*>(previous);
            bytes[offset] = bytes[offset] ^ 1;
            TCMallocInternalFree(ptr);
            _exit(2);
        }
        buckets[bucket] = ptr;
    }
    _exit(3);
}
}

Y_UNIT_TEST_SUITE(TCMallocFreeCheck) {
    Y_UNIT_TEST(DoubleFree) {
        CheckFailure([] {
            void* ptr = TCMallocInternalMalloc(128);
            TCMallocInternalFree(ptr);
            TCMallocInternalFree(ptr);
        }, "Free check: double free");
    }

    Y_UNIT_TEST(SizedDoubleFree) {
        CheckFailure([] {
            void* ptr = TCMallocInternalMalloc(128);
            TCMallocInternalFreeSized(ptr, 128);
            TCMallocInternalFreeSized(ptr, 128);
        }, "Free check: double free");
    }

    Y_UNIT_TEST(WriteAfterFreeHead) {
        CheckFailure([] { CorruptAndEvict(0); }, "Free check: write after free");
    }

    Y_UNIT_TEST(WriteAfterFreeTail) {
        CheckFailure([] { CorruptAndEvict(255); }, "Free check: write after free");
    }

    Y_UNIT_TEST(ConcurrentChurnAndFork) {
        MallocExtension_EnableForkSupport();
        std::vector<std::thread> threads;
        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([] {
                for (size_t i = 0; i < 20000; ++i) {
                    const size_t size = 1 + i % 40000;
                    void* ptr = TCMallocInternalMalloc(size);
                    memset(ptr, 42, size);
                    TCMallocInternalFree(ptr);
                }
            });
        }
        const pid_t pid = fork();
        if (pid == 0) {
            for (int i = 0; i < 1000; ++i) {
                TCMallocInternalFree(TCMallocInternalMalloc(128));
            }
            _exit(0);
        }
        for (auto& thread : threads) thread.join();
        UNIT_ASSERT(pid >= 0);
        int status = 0;
        UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, 0), pid);
        UNIT_ASSERT(WIFEXITED(status));
        UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), 0);
        TCMallocInternalFree(nullptr);
    }
}
