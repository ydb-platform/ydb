#pragma once

#include <vector>

#include <util/system/type_name.h>
#include <util/thread/singleton.h>

#define ENABLE_MEMORY_TRACKING

namespace NActors {
namespace NMemory {

namespace NPrivate {

class TMetric {
    std::atomic<ssize_t> Memory;
    std::atomic<ssize_t> Count;

    void Copy(const TMetric& other) {
        Memory.store(other.GetMemory(), std::memory_order_relaxed);
        Count.store(other.GetCount(), std::memory_order_relaxed);
    }

public:
    TMetric()
        : Memory(0)
        , Count(0)
    {}

    inline TMetric(const TMetric& other) {
        Copy(other);
    }

    inline TMetric(TMetric&& other) {
        Copy(other);
    }

    inline TMetric& operator=(const TMetric& other) {
        Copy(other);
        return *this;
    }

    inline TMetric& operator=(TMetric&& other) {
        Copy(other);
        return *this;
    }

    inline ssize_t GetMemory() const {
        return Memory.load(std::memory_order_relaxed);
    }
    inline void SetMemory(ssize_t value) {
        Memory.store(value, std::memory_order_relaxed);
    }

    inline ssize_t GetCount() const {
        return Count.load(std::memory_order_relaxed);
    }
    inline void SetCount(ssize_t value) {
        Count.store(value, std::memory_order_relaxed);
    }

    inline void operator+=(const TMetric& other) {
        SetMemory(GetMemory() + other.GetMemory());
        SetCount(GetCount() + other.GetCount());
    }

    inline void CalculatePeak(const TMetric& other) {
        SetMemory(Max(GetMemory(), other.GetMemory()));
        SetCount(Max(GetCount(), other.GetCount()));
    }

    inline void Add(size_t size) {
        SetMemory(GetMemory() + size);
        SetCount(GetCount() + 1);
    }

    inline void Sub(size_t size) {
        SetMemory(GetMemory() - size);
        SetCount(GetCount() - 1);
    }
};


class TThreadLocalInfo {
public:
    TThreadLocalInfo();
    ~TThreadLocalInfo();

    TMetric* GetMetric(size_t index);
    const std::vector<TMetric>& GetMetrics() const;

private:
    std::vector<TMetric> Metrics;

    inline static TMetric Null = {};
};


class TBaseLabel {
protected:
    static size_t RegisterStaticMemoryLabel(const char* name, bool hasSensor);

    inline static TMetric* GetLocalMetric(size_t index) {
        return FastTlsSingleton<TThreadLocalInfo>()->GetMetric(index);
    }
};


template <const char* Name>
class TNameLabel
    : TBaseLabel
{
public:
    static void Add(size_t size) {
#if defined(ENABLE_MEMORY_TRACKING)
        Y_UNUSED(MetricInit);

        if (Y_UNLIKELY(!Metric)) {
            Metric = GetLocalMetric(Index);
        }

        Metric->Add(size);
#else
        Y_UNUSED(size);
#endif
    }

    static void Sub(size_t size) {
#if defined(ENABLE_MEMORY_TRACKING)
        Y_UNUSED(MetricInit);

        if (Y_UNLIKELY(!Metric)) {
            Metric = GetLocalMetric(Index);
        }

        Metric->Sub(size);
#else
        Y_UNUSED(size);
#endif
    }

private:
#if defined(ENABLE_MEMORY_TRACKING)
    inline static size_t Index = Max<size_t>();
    inline static struct TMetricInit {
        TMetricInit() {
            Index = RegisterStaticMemoryLabel(Name, true);
        }
    } MetricInit;

    inline static thread_local TMetric* Metric = nullptr;
#endif
};


template <typename TType>
class TTypeLabel
    : TBaseLabel
{
public:
    static void Add(size_t size) {
#if defined(ENABLE_MEMORY_TRACKING)
        Y_UNUSED(MetricInit);

        if (Y_UNLIKELY(!Metric)) {
            Metric = GetLocalMetric(Index);
        }

        Metric->Add(size);
#else
        Y_UNUSED(size);
#endif
    }

    static void Sub(size_t size) {
#if defined(ENABLE_MEMORY_TRACKING)
        Y_UNUSED(MetricInit);

        if (Y_UNLIKELY(!Metric)) {
            Metric = GetLocalMetric(Index);
        }

        Metric->Sub(size);
#else
        Y_UNUSED(size);
#endif
    }

private:
#if defined(ENABLE_MEMORY_TRACKING)
    inline static size_t Index = Max<size_t>();
    inline static struct TMetricInit {
        TMetricInit() {
            Index = RegisterStaticMemoryLabel(TypeName<TType>().c_str(), false);
        }
    } MetricInit;

    inline static thread_local TMetric* Metric = nullptr;
#endif
};


template <typename T>
struct TTrackHelper {
#if defined(ENABLE_MEMORY_TRACKING)
    void* operator new(size_t size) {
        T::Add(size);
        return malloc(size);
    }

    void* operator new[](size_t size) {
        T::Add(size);
        return malloc(size);
    }

    void operator delete(void* ptr, size_t size) {
        T::Sub(size);
        free(ptr);
    }

    void operator delete[](void* ptr, size_t size) {
        T::Sub(size);
        free(ptr);
    }
#endif
};

template <typename TType, typename T>
struct TAllocHelper {
    typedef size_t size_type;
    typedef TType value_type;
    typedef TType* pointer;
    typedef const TType* const_pointer;

    struct propagate_on_container_copy_assignment : public std::false_type {};
    struct propagate_on_container_move_assignment : public std::false_type {};
    struct propagate_on_container_swap : public std::false_type {};

    pointer allocate(size_type n, const void* hint = nullptr) {
        Y_UNUSED(hint);
        auto size = n * sizeof(TType);
        T::Add(size);
        return (pointer)malloc(size);
    }

    void deallocate(pointer ptr, size_t n) {
        auto size = n * sizeof(TType);
        T::Sub(size);
        free((void*)ptr);
    }
};

} // NPrivate


template <const char* Name>
using TLabel = NPrivate::TNameLabel<Name>;

template <typename TType, const char* Name = nullptr>
struct TTrack
    : public NPrivate::TTrackHelper<NPrivate::TNameLabel<Name>>
{
};

template <typename TType>
struct TTrack<TType, nullptr>
    : public NPrivate::TTrackHelper<NPrivate::TTypeLabel<TType>>
{
};

template <typename TType, const char* Name = nullptr>
struct TAlloc
    : public NPrivate::TAllocHelper<TType, NPrivate::TNameLabel<Name>>
{
    template<typename U>
    struct rebind {
        typedef TAlloc<U, Name> other;
    };
};

template <typename TType>
struct TAlloc<TType, nullptr>
    : public NPrivate::TAllocHelper<TType, NPrivate::TTypeLabel<TType>>
{
    template<typename U>
    struct rebind {
        typedef TAlloc<U> other;
    };
};

}
}

