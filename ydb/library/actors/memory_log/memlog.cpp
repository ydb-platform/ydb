#include "memlog.h"

#include <ydb/library/actors/util/datetime.h>

#include <util/system/info.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/align.h>

#include <contrib/libs/linuxvdso/interface.h>

#if (defined(_i386_) || defined(_x86_64_)) && defined(_linux_)
#define HAVE_VDSO_GETCPU 1
#include <contrib/libs/linuxvdso/interface.h>
static int (*FastGetCpu)(unsigned* cpu, unsigned* node, void* unused);
#endif

#if defined(_unix_)
#include <sched.h>
#elif defined(_win_)
#include <WinBase.h>
#else
#error NO IMPLEMENTATION FOR THE PLATFORM
#endif

const char TMemoryLog::DEFAULT_LAST_MARK[16] = {
    'c',
    'b',
    '7',
    'B',
    '6',
    '8',
    'a',
    '8',
    'A',
    '5',
    '6',
    '1',
    '6',
    '4',
    '5',
    '\n',
};

const char TMemoryLog::CLEAR_MARK[16] = {
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    ' ',
    '\n',
};

unsigned TMemoryLog::GetSelfCpu() noexcept {
#if defined(_unix_)
#if HAVE_VDSO_GETCPU
    unsigned cpu;
    if (Y_LIKELY(FastGetCpu != nullptr)) {
        auto result = FastGetCpu(&cpu, nullptr, nullptr);
        Y_ABORT_UNLESS(result == 0);
        return cpu;
    } else {
        return 0;
    }

#elif defined(_x86_64_) || defined(_i386_)

#define CPUID(func, eax, ebx, ecx, edx)              \
    __asm__ __volatile__(                            \
        "cpuid"                                      \
        : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) \
        : "a"(func));

    int a = 0, b = 0, c = 0, d = 0;
    CPUID(0x1, a, b, c, d);
    int acpiID = (b >> 24);
    return acpiID;

#elif defined(__CNUC__)
    return sched_getcpu();
#else
    return 0;
#endif

#elif defined(_win_)
    return GetCurrentProcessorNumber();
#else
    return 0;
#endif
}

std::atomic<TMemoryLog*> TMemoryLog::MemLogBuffer = nullptr;
Y_POD_THREAD(TThread::TId)
TMemoryLog::LogThreadId;
char* TMemoryLog::LastMarkIsHere = nullptr;

std::atomic<bool> TMemoryLog::PrintLastMark(true);

TMemoryLog::TMemoryLog(size_t totalSize, size_t grainSize)
    : GrainSize(grainSize)
    , FreeGrains(DEFAULT_TOTAL_SIZE / DEFAULT_GRAIN_SIZE * 2)
    , Buf(totalSize)
{
    Y_ABORT_UNLESS(DEFAULT_TOTAL_SIZE % DEFAULT_GRAIN_SIZE == 0);
    NumberOfGrains = DEFAULT_TOTAL_SIZE / DEFAULT_GRAIN_SIZE;

    for (size_t i = 0; i < NumberOfGrains; ++i) {
        new (GetGrain(i)) TGrain;
    }

    NumberOfCpus = NSystemInfo::NumberOfCpus();
    Y_ABORT_UNLESS(NumberOfGrains > NumberOfCpus);
    ActiveGrains.Reset(new TGrain*[NumberOfCpus]);
    for (size_t i = 0; i < NumberOfCpus; ++i) {
        ActiveGrains[i] = GetGrain(i);
    }

    for (size_t i = NumberOfCpus; i < NumberOfGrains; ++i) {
        FreeGrains.StubbornPush(GetGrain(i));
    }

#if HAVE_VDSO_GETCPU
    auto vdsoFunc = (decltype(FastGetCpu))
        NVdso::Function("__vdso_getcpu", "LINUX_2.6");
    AtomicSet(FastGetCpu, vdsoFunc);
#endif
}

void* TMemoryLog::GetWriteBuffer(size_t amount) noexcept {
    // alignment required by NoCacheMemcpy
    amount = AlignUp<size_t>(amount, MemcpyAlignment);

    for (ui16 tries = MAX_GET_BUFFER_TRIES; tries-- > 0;) {
        auto myCpu = GetSelfCpu();

        TGrain* grain = AtomicGet(ActiveGrains[myCpu]);

        if (grain != nullptr) {
            auto mine = AtomicGetAndAdd(grain->WritePointer, amount);
            if (mine + amount <= GrainSize - sizeof(TGrain)) {
                return &grain->Data[mine];
            }

            if (!AtomicCas(&ActiveGrains[myCpu], 0, grain)) {
                continue;
            }

            FreeGrains.StubbornPush(grain);
        }

        grain = (TGrain*)FreeGrains.Pop();

        if (grain == nullptr) {
            return nullptr;
        }

        grain->WritePointer = 0;

        if (!AtomicCas(&ActiveGrains[myCpu], grain, 0)) {
            FreeGrains.StubbornPush(grain);
            continue;
        }
    }

    return nullptr;
}

void ClearAlignedTail(char* tail) noexcept {
    auto aligned = AlignUp(tail, TMemoryLog::MemcpyAlignment);
    if (aligned > tail) {
        memset(tail, 0, aligned - tail);
    }
}

#if defined(_x86_64_) || defined(_i386_)
#include <xmmintrin.h>
// the main motivation is not poluting CPU cache
NO_SANITIZE_THREAD
void NoCacheMemcpy(char* dst, const char* src, size_t size) noexcept {
    while (size >= sizeof(__m128) * 2) {
        __m128 a = _mm_load_ps((float*)(src + 0 * sizeof(__m128)));
        __m128 b = _mm_load_ps((float*)(src + 1 * sizeof(__m128)));
        _mm_stream_ps((float*)(dst + 0 * sizeof(__m128)), a);
        _mm_stream_ps((float*)(dst + 1 * sizeof(__m128)), b);

        size -= sizeof(__m128) * 2;
        src += sizeof(__m128) * 2;
        dst += sizeof(__m128) * 2;
    }
    memcpy(dst, src, size);
}

NO_SANITIZE_THREAD
void NoWCacheMemcpy(char* dst, const char* src, size_t size) noexcept {
    constexpr ui16 ITEMS_COUNT = 1024;
    alignas(TMemoryLog::MemcpyAlignment) __m128 buf[ITEMS_COUNT];
    while (size >= sizeof(buf)) {
        memcpy(&buf, src, sizeof(buf));

        for (ui16 i = 0; i < ITEMS_COUNT; ++i) {
            _mm_stream_ps((float*)dst, buf[i]);
            dst += sizeof(__m128);
        }

        size -= sizeof(buf);
        src += sizeof(buf);
    }

    memcpy(&buf, src, size);
    // no problem to copy few bytes more
    size = AlignUp(size, sizeof(__m128));
    for (ui16 i = 0; i < size / sizeof(__m128); ++i) {
        _mm_stream_ps((float*)dst, buf[i]);
        dst += sizeof(__m128);
    }
}

#endif

NO_SANITIZE_THREAD
char* BareMemLogWrite(const char* begin, size_t msgSize, bool isLast) noexcept {
    bool lastMark =
        isLast && TMemoryLog::PrintLastMark.load(std::memory_order_acquire);
    size_t amount = lastMark ? msgSize + TMemoryLog::LAST_MARK_SIZE : msgSize;

    char* buffer = (char*)TMemoryLog::GetWriteBufferStatic(amount);
    if (buffer == nullptr) {
        return nullptr;
    }

#if defined(_x86_64_) || defined(_i386_)
    if (AlignDown(begin, TMemoryLog::MemcpyAlignment) == begin) {
        NoCacheMemcpy(buffer, begin, msgSize);
    } else {
        NoWCacheMemcpy(buffer, begin, msgSize);
    }
#else
    memcpy(buffer, begin, msgSize);
#endif

    if (lastMark) {
        TMemoryLog::ChangeLastMark(buffer + msgSize);
    }

    ClearAlignedTail(buffer + amount);
    return buffer;
}

NO_SANITIZE_THREAD
bool MemLogWrite(const char* begin, size_t msgSize, bool addLF) noexcept {
    bool lastMark = TMemoryLog::PrintLastMark.load(std::memory_order_acquire);
    size_t amount = lastMark ? msgSize + TMemoryLog::LAST_MARK_SIZE : msgSize;

    // Let's construct prolog with timestamp and thread id
    auto threadId = TMemoryLog::GetTheadId();

    // alignment required by NoCacheMemcpy
    // check for format for snprintf
    constexpr size_t prologSize = 48;
    alignas(TMemoryLog::MemcpyAlignment) char prolog[prologSize + 1];
    Y_ABORT_UNLESS(AlignDown(&prolog, TMemoryLog::MemcpyAlignment) == &prolog);

    int snprintfResult = snprintf(prolog, prologSize + 1,
                                  "TS %020" PRIu64 " TI %020" PRIu64 " ", GetCycleCountFast(), threadId);

    if (snprintfResult < 0) {
        return false;
    }
    Y_ABORT_UNLESS(snprintfResult == prologSize);

    amount += prologSize;
    if (addLF) {
        ++amount; // add 1 byte for \n at the end of the message
    }

    char* buffer = (char*)TMemoryLog::GetWriteBufferStatic(amount);
    if (buffer == nullptr) {
        return false;
    }

#if defined(_x86_64_) || defined(_i386_)
    // warning: copy prolog first to avoid corruption of the message
    // by prolog tail
    NoCacheMemcpy(buffer, prolog, prologSize);
    if (AlignDown(begin + prologSize, TMemoryLog::MemcpyAlignment) == begin + prologSize) {
        NoCacheMemcpy(buffer + prologSize, begin, msgSize);
    } else {
        NoWCacheMemcpy(buffer + prologSize, begin, msgSize);
    }
#else
    memcpy(buffer, prolog, prologSize);
    memcpy(buffer + prologSize, begin, msgSize);
#endif

    if (addLF) {
        buffer[prologSize + msgSize] = '\n';
    }

    if (lastMark) {
        TMemoryLog::ChangeLastMark(buffer + prologSize + msgSize + (int)addLF);
    }

    ClearAlignedTail(buffer + amount);
    return true;
}

NO_SANITIZE_THREAD
void TMemoryLog::ChangeLastMark(char* buffer) noexcept {
    memcpy(buffer, DEFAULT_LAST_MARK, LAST_MARK_SIZE);
    auto oldMark = AtomicSwap(&LastMarkIsHere, buffer);
    if (Y_LIKELY(oldMark != nullptr)) {
        memcpy(oldMark, CLEAR_MARK, LAST_MARK_SIZE);
    }
    if (AtomicGet(LastMarkIsHere) != buffer) {
        memcpy(buffer, CLEAR_MARK, LAST_MARK_SIZE);
        AtomicBarrier();
    }
}

bool MemLogVPrintF(const char* format, va_list params) noexcept {
    auto logger = TMemoryLog::GetMemoryLogger();
    if (logger == nullptr) {
        return false;
    }

    auto threadId = TMemoryLog::GetTheadId();

    // alignment required by NoCacheMemcpy
    alignas(TMemoryLog::MemcpyAlignment) char buf[TMemoryLog::MAX_MESSAGE_SIZE];
    Y_ABORT_UNLESS(AlignDown(&buf, TMemoryLog::MemcpyAlignment) == &buf);

    int prologSize = snprintf(buf,
                              TMemoryLog::MAX_MESSAGE_SIZE - 2,
                              "TS %020" PRIu64 " TI %020" PRIu64 " ",
                              GetCycleCountFast(),
                              threadId);

    if (Y_UNLIKELY(prologSize < 0)) {
        return false;
    }
    Y_ABORT_UNLESS((ui32)prologSize <= TMemoryLog::MAX_MESSAGE_SIZE);

    int add = vsnprintf(
        &buf[prologSize],
        TMemoryLog::MAX_MESSAGE_SIZE - prologSize - 2,
        format, params);

    if (Y_UNLIKELY(add < 0)) {
        return false;
    }
    Y_ABORT_UNLESS(add >= 0);
    auto totalSize = prologSize + add;

    buf[totalSize++] = '\n';
    Y_ABORT_UNLESS((ui32)totalSize <= TMemoryLog::MAX_MESSAGE_SIZE);

    return BareMemLogWrite(buf, totalSize) != nullptr;
}
