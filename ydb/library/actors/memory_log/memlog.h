#pragma once

#include <library/cpp/threading/queue/mpmc_unordered_ring.h>
#include <util/generic/string.h>
#include <util/string/printf.h>
#include <util/system/datetime.h>
#include <util/system/thread.h>
#include <util/system/types.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/align.h>
#include <util/system/tls.h>

#include <atomic>
#include <cstdio>

#ifdef _win_
#include <util/system/winint.h>
#endif

#ifndef NO_SANITIZE_THREAD
#define NO_SANITIZE_THREAD
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#undef NO_SANITIZE_THREAD
#define NO_SANITIZE_THREAD __attribute__((no_sanitize_thread))
#endif
#endif
#endif

class TMemoryLog {
public:
    static constexpr size_t DEFAULT_TOTAL_SIZE = 10 * 1024 * 1024;
    static constexpr size_t DEFAULT_GRAIN_SIZE = 1024 * 64;
    static constexpr size_t MAX_MESSAGE_SIZE = 1024;
    static constexpr ui16 MAX_GET_BUFFER_TRIES = 4;
    static constexpr ui16 MemcpyAlignment = 16;

    // search for cb7B68a8A561645
    static const char DEFAULT_LAST_MARK[16];
    static const char CLEAR_MARK[16];

    static constexpr size_t LAST_MARK_SIZE = sizeof(DEFAULT_LAST_MARK);

    inline static TMemoryLog* GetMemoryLogger() noexcept {
        return MemLogBuffer.load(std::memory_order_acquire);
    }

    void* GetWriteBuffer(size_t amount) noexcept;

    inline static void* GetWriteBufferStatic(size_t amount) noexcept {
        auto logger = GetMemoryLogger();
        if (logger == nullptr) {
            return nullptr;
        }
        return logger->GetWriteBuffer(amount);
    }

    size_t GetGlobalBufferSize() const noexcept {
        return Buf.GetSize();
    }

    inline static void CreateMemoryLogBuffer(
        size_t totalSize = DEFAULT_TOTAL_SIZE,
        size_t grainSize = DEFAULT_GRAIN_SIZE)
        Y_COLD {
        if (MemLogBuffer.load(std::memory_order_acquire) != nullptr) {
            return;
        }

        MemLogBuffer.store(new TMemoryLog(totalSize, grainSize), std::memory_order_release);
    }

    static std::atomic<bool> PrintLastMark;

    // buffer must be at least 16 bytes
    static void ChangeLastMark(char* buffer) noexcept;

    inline static TThread::TId GetTheadId() noexcept {
        if (LogThreadId == 0) {
            LogThreadId = TThread::CurrentThreadId();
        }
        return LogThreadId;
    }

private:
    TMemoryLog(size_t totalSize, size_t grainSize) Y_COLD;

    struct TGrain {
        TAtomic WritePointer = 0;
        char Padding[MemcpyAlignment - sizeof(TAtomic)];
        char Data[];
    };

    size_t NumberOfCpus;
    size_t GrainSize;
    size_t NumberOfGrains;
    TArrayPtr<TGrain*> ActiveGrains;
    NThreading::TMPMCUnorderedRing FreeGrains;

    TGrain* GetGrain(size_t grainIndex) const noexcept {
        return (TGrain*)((char*)GetGlobalBuffer() + GrainSize * grainIndex);
    }

    class TMMapArea {
    public:
        TMMapArea(size_t amount) Y_COLD {
            MMap(amount);
        }

        TMMapArea(const TMMapArea&) = delete;
        TMMapArea& operator=(const TMMapArea& copy) = delete;

        TMMapArea(TMMapArea&& move) Y_COLD {
            BufPtr = move.BufPtr;
            Size = move.Size;

            move.BufPtr = nullptr;
            move.Size = 0;
        }

        TMMapArea& operator=(TMMapArea&& move) Y_COLD {
            BufPtr = move.BufPtr;
            Size = move.Size;

            move.BufPtr = nullptr;
            move.Size = 0;
            return *this;
        }

        void Reset(size_t amount) Y_COLD {
            MUnmap();
            MMap(amount);
        }

        ~TMMapArea() noexcept Y_COLD {
            MUnmap();
        }

        size_t GetSize() const noexcept {
            return Size;
        }

        void* GetPtr() const noexcept {
            return BufPtr;
        }

    private:
        void* BufPtr;
        size_t Size;
#ifdef _win_
        HANDLE Mapping;
#endif

        void MMap(size_t amount);
        void MUnmap();
    };

    TMMapArea Buf;

    void* GetGlobalBuffer() const noexcept {
        return Buf.GetPtr();
    }

    static unsigned GetSelfCpu() noexcept;

    static std::atomic<TMemoryLog*> MemLogBuffer;
    static Y_POD_THREAD(TThread::TId) LogThreadId;
    static char* LastMarkIsHere;
};

// it's no use of sanitizing this function
NO_SANITIZE_THREAD
char* BareMemLogWrite(
    const char* begin, size_t msgSize, bool isLast = true) noexcept;

// it's no use of sanitizing this function
NO_SANITIZE_THREAD
bool MemLogWrite(
    const char* begin, size_t msgSize, bool addLF = false) noexcept;

Y_WRAPPER inline bool MemLogWrite(const char* begin, const char* end) noexcept {
    if (end <= begin) {
        return false;
    }

    size_t msgSize = end - begin;
    return MemLogWrite(begin, msgSize);
}

template <typename TObj>
bool MemLogWriteStruct(const TObj* obj) noexcept {
    auto begin = (const char*)(const void*)obj;
    return MemLogWrite(begin, begin + sizeof(TObj));
}

Y_PRINTF_FORMAT(1, 0)
bool MemLogVPrintF(const char* format, va_list params) noexcept;

Y_PRINTF_FORMAT(1, 2)
Y_WRAPPER
inline bool MemLogPrintF(const char* format, ...) noexcept {
    va_list params;
    va_start(params, format);
    auto result = MemLogVPrintF(format, params);
    va_end(params);
    return result;
}

Y_WRAPPER inline bool MemLogWriteNullTerm(const char* str) noexcept {
    return MemLogWrite(str, strlen(str));
}
