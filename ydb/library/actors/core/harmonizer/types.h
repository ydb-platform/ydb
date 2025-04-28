#pragma once

namespace NActors {


template <typename T>
class TIterableRange {
private:
    T* Begin = nullptr;
    T* End = nullptr;

public:
    TIterableRange(T* begin, T* end)
        : Begin(begin)
        , End(end)
    {
    }
    
    TIterableRange() = default;

    void Set(T* begin, T* end) {
        Begin = begin;
        End = end;
    }

    T* begin() const {
        return Begin;
    }

    T* end() const {
        return End;
    }

    bool empty() const {
        return Begin == End;
    }

    size_t size() const {
        return End - Begin;
    }

    T& operator[](size_t index) const {
        return Begin[index];
    }

    T& front() const {
        return *Begin;
    }

    T& back() const {
        return *(End - 1);
    }
};

template <typename T>
class TIterableDoubleRange {
private:
    using TSelf = TIterableDoubleRange<T>;

    T* FirstBegin = nullptr;
    T* FirstEnd = nullptr;
    T* SecondBegin = nullptr;
    T* SecondEnd = nullptr;

public:
    struct iterator : public std::iterator<
            std::random_access_iterator_tag, // iterator_category
            T,                    // value_type
            size_t,               // difference_type
            const T*,             // pointer
            T                     // reference
        >
    {
        TSelf* Range = nullptr;
        size_t Index = 0;

        iterator(TSelf* range, size_t index) : Range(range), Index(index) {}

        T& operator*() const {
            return (*Range)[Index];
        }

        T& operator->() const {
            return (*Range)[Index];
        }

        iterator& operator++() {
            ++Index;
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++Index;
            return tmp;
        }

        iterator& operator--() {
            --Index;
            return *this;
        }

        iterator operator--(int) {
            iterator tmp = *this;
            --Index;
            return tmp;
        }

        iterator operator+(size_t offset) const {
            return iterator(Range, Index + offset);
        }

        iterator& operator+=(size_t offset) {
            Index += offset;
            return *this;
        }

        iterator operator-(size_t offset) const {
            return iterator(Range, Index - offset);
        }

        iterator& operator-=(size_t offset) {
            Index -= offset;
            return *this;
        }

        size_t operator-(const iterator& other) const {
            return Index - other.Index;
        }

        auto operator<=>(const iterator&) const = default;
    };

    struct const_iterator : public std::iterator<
            std::random_access_iterator_tag, // iterator_category
            T,                    // value_type
            size_t,               // difference_type
            const T*,             // pointer
            T                     // reference
        >
    {
        const TSelf* Range = nullptr;
        size_t Index = 0;

        const_iterator(const TSelf* range, size_t index) : Range(range), Index(index) {}

        const T& operator*() const {
            return (*Range)[Index];
        }

        const T& operator->() const {
            return (*Range)[Index];
        }

        const_iterator& operator++() {
            ++Index;
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++Index;
            return tmp;
        }

        const_iterator& operator--() {
            --Index;
            return *this;
        }

        const_iterator operator--(int) {
            const_iterator tmp = *this;
            --Index;
            return tmp;
        }

        const_iterator operator+(size_t offset) const {
            return const_iterator(Range, Index + offset);
        }

        const_iterator& operator+=(size_t offset) {
            Index += offset;
            return *this;
        }

        const_iterator operator-(size_t offset) const {
            return const_iterator(Range, Index - offset);
        }

        const_iterator& operator-=(size_t offset) {
            Index -= offset;
            return *this;
        }

        size_t operator-(const const_iterator& other) const {
            return Index - other.Index;
        }

        auto operator<=>(const const_iterator&) const = default;
    };

public:
    TIterableDoubleRange(T* firstBegin, T* firstEnd, T* secondBegin, T* secondEnd)
        : FirstBegin(firstBegin)
        , FirstEnd(firstEnd)
        , SecondBegin(secondBegin)
        , SecondEnd(secondEnd)
    {
    }

    iterator begin() {
        return iterator(this, 0);
    }

    const_iterator begin() const {
        return const_iterator(this, 0);
    }

    iterator end() {
        return iterator(this, size());
    }

    const_iterator end() const {
        return const_iterator(this, size());
    }

    T& operator[](size_t index) const {
        size_t firstSize = FirstEnd - FirstBegin;
        if (index < firstSize) {
            return FirstBegin[index];
        }
        return SecondBegin[index - firstSize];
    }

    T& front() const {
        return *FirstBegin;
    }

    T& back() const {
        return *(--end());
    }

    size_t size() const {
        return FirstEnd - FirstBegin + SecondEnd - SecondBegin;
    }
};

// 8 bytes
struct THarmonizerIterationOperation {
    struct NoOperation {
    };

    struct IncreaseThreadByNeedyState {
    };

    struct IncreaseThreadByExchange {
    };

    struct DecreaseThreadByStarvedState {
        i16 ThreadCount;
    };

    struct DecreaseThreadByHoggishState {
    };

    struct DecreaseThreadByExchange {
    };

    struct IncreaseForeignSlotsInSharedPool {
    };

    struct DecreaseForeignSlotsInSharedPool {
    };

    std::variant<
        NoOperation,
        IncreaseThreadByNeedyState,
        IncreaseThreadByExchange,
        DecreaseThreadByStarvedState,
        DecreaseThreadByHoggishState,
        DecreaseThreadByExchange,
        IncreaseForeignSlotsInSharedPool,
        DecreaseForeignSlotsInSharedPool> Type;

    TString ToString() const {
        return std::visit([](auto &&arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, NoOperation>) {
                return "NoOperation";
            } else if constexpr (std::is_same_v<T, IncreaseThreadByNeedyState>) {
                return "IncreaseThreadByNeedyState";
            } else if constexpr (std::is_same_v<T, IncreaseThreadByExchange>) {
                return "IncreaseThreadByExchange";
            } else if constexpr (std::is_same_v<T, DecreaseThreadByStarvedState>) {
                return "DecreaseThreadByStarvedState";
            } else if constexpr (std::is_same_v<T, DecreaseThreadByHoggishState>) {
                return "DecreaseThreadByHoggishState";
            } else if constexpr (std::is_same_v<T, DecreaseThreadByExchange>) {
                return "DecreaseThreadByExchange";
            } else if constexpr (std::is_same_v<T, IncreaseForeignSlotsInSharedPool>) {
                return "IncreaseForeignSlotsInSharedPool";
            } else if constexpr (std::is_same_v<T, DecreaseForeignSlotsInSharedPool>) {
                return "DecreaseForeignSlotsInSharedPool";
            }
            return "Unknown";
        }, Type);
    };
};

// 16 bytes
struct TCpuStats {
    float Cpu = 0.0;
    float LastSecondCpu = 0.0;
};

// 32 bytes
struct THarmonizerIterationThreadState {
    TCpuStats UsedCpu;
    TCpuStats ElapsedCpu;
    TCpuStats ParkedCpu;
};

struct THarmonizerPersistentPoolState {
    float DefaultThreadCount = 0.0;
    float MinThreadCount = 0.0;
    float MaxThreadCount = 0.0;
    i16 DefaultFullThreadCount = 0;
    i16 MinFullThreadCount = 0;
    i16 MaxFullThreadCount = 0;

    i16 Priority = 0;
    ui16 MaxLocalQueueSize = 0;
    ui16 MinLocalQueueSize = 0;
    TString Name;
};

// 48 bytes + 320bytes
struct THarmonizerIterationPoolState {
    THarmonizerPersistentPoolState* PersistentState = nullptr;
    ui64 AvgPingUs = 0;
    ui64 AvgPingUsWithSmallWindow = 0;
    ui64 MaxAvgPingUs = 0;
    THarmonizerIterationOperation Operation;
    float PotentialMaxThreadCount = 0.0;
    float CurrentThreadCount = 0;

    ui16 LocalQueueSize = 0;
    bool IsNeedy = false;
    bool IsStarved = false;
    bool IsHoggish = false;

    TIterableRange<THarmonizerIterationThreadState> Threads; // x10
};

// 16 bytes + 160 bytes
struct THarmonizerIterationSharedThreadState {

    TIterableRange<THarmonizerIterationThreadState> ByPool; // x5
};

// 16 bytes + 800 bytes
struct THarmonizerIterationSharedPoolState {

    TIterableRange<THarmonizerIterationSharedThreadState> Threads; // x5
};

// 64 bytes + 800 bytes + 1600 bytes = 2464
struct THarmonizerIterationState {
    ui64 Iteration;
    ui64 Ts;

    float Budget = 0.0;
    float LostCpu = 0.0;
    float FreeSharedCpu = 0.0;

    TIterableRange<THarmonizerIterationPoolState> Pools; // x5
    THarmonizerIterationSharedPoolState Shared;
};

} // NActors
