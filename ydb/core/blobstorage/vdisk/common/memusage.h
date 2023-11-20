#pragma once

#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/generic/list.h>
#include <util/generic/vector.h>

namespace NKikimr {

    class TMemoryConsumer {
        ::NMonitoring::TDynamicCounters::TCounterPtr Counter;

    public:
        TMemoryConsumer(::NMonitoring::TDynamicCounters::TCounterPtr counter)
            : Counter(std::move(counter))
        {}

        TMemoryConsumer(TMemoryConsumer&& other) = default;
        explicit TMemoryConsumer(const TMemoryConsumer& other) = default;

        void Add(size_t bytes) {
            Delta(bytes);
        }

        void Subtract(size_t bytes) {
            Delta(-static_cast<ssize_t>(bytes));
        }

        void Delta(ssize_t bytes) {
            *Counter += static_cast<i64>(bytes);
        }

        ::NMonitoring::TDynamicCounters::TCounterPtr GetCounter() const {
            return Counter;
        }
    };

    class TMemoryConsumerWithDropOnDestroy : protected TMemoryConsumer {
    public:
        TMemoryConsumerWithDropOnDestroy(::NMonitoring::TDynamicCounters::TCounterPtr counter)
            : TMemoryConsumer(counter)
        {}
        TMemoryConsumerWithDropOnDestroy(const TMemoryConsumer &other)
            : TMemoryConsumer(other)
        {}
        TMemoryConsumerWithDropOnDestroy(TMemoryConsumerWithDropOnDestroy&& other) = default;
        explicit TMemoryConsumerWithDropOnDestroy(const TMemoryConsumerWithDropOnDestroy& other) = default;

        ~TMemoryConsumerWithDropOnDestroy() {
            TMemoryConsumer::Subtract(MemConsumed);
        }

        void Add(size_t bytes) {
            this->Delta(bytes);
        }

        void Subtract(size_t bytes) {
            this->Delta(-static_cast<ssize_t>(bytes));
        }

        void Delta(ssize_t bytes) {
            TMemoryConsumer::Delta(bytes);
            MemConsumed += bytes;
            Y_DEBUG_ABORT_UNLESS(MemConsumed >= 0);
        }

        using TMemoryConsumer::GetCounter;

    private:
        ssize_t MemConsumed = 0;
    };

    template<typename TDerived>
    class TTrackableBase
    {
        TMemoryConsumer Consumer;
#ifndef NDEBUG
        size_t LastCapacity;
#endif

        size_t GetCapacity() const {
            return static_cast<const TDerived&>(*this).GetCapacityImpl();
        }

        void CountInitialCapacity() {
            size_t capacity = GetCapacity();
#ifndef NDEBUG
            LastCapacity = capacity;
#endif
            Consumer.Add(capacity);
        }

        class TTracker
        {
            TTrackableBase& Base;
            size_t InitialCapacity;

        public:
            TTracker(TTrackableBase& base)
                : Base(base)
                , InitialCapacity(Base.GetCapacity())
            {
#ifndef NDEBUG
                Y_ABORT_UNLESS(InitialCapacity == Base.LastCapacity, "InitialCapacity# %zu Base.LastCapacity# %zu",
                        InitialCapacity, Base.LastCapacity);
#endif
            }

            ~TTracker() {
                size_t capacity = Base.GetCapacity();
                ssize_t delta = capacity - InitialCapacity;
                Base.Consumer.Delta(delta);
#ifndef NDEBUG
                Base.LastCapacity = capacity;
#endif
            }
        };

    public:
        TTrackableBase(TMemoryConsumer&& consumer)
            : Consumer(std::move(consumer))
        {
            CountInitialCapacity();
        }

        TTrackableBase(const TTrackableBase& other)
            : Consumer(other.Consumer)
        {
            CountInitialCapacity();
        }

        ~TTrackableBase() {
            size_t capacity = GetCapacity();
#ifndef NDEBUG
            Y_ABORT_UNLESS(capacity == LastCapacity, "capacity# %zu LastCapacity# %zu", capacity, LastCapacity);
#endif
            Consumer.Subtract(capacity);
        }

        TTrackableBase& operator =(const TTrackableBase& other) {
            Consumer = other.Consumer;
            CountInitialCapacity();
            return *this;
        }

        TTracker SpawnTracker() {
            return TTracker(*this);
        }
    };

    template<typename T>
    class THasCapitalSwapMethod {
        template<typename C>
        static std::true_type HasMethodImpl(decltype(&C::Swap)*);
        template<typename C>
        static std::false_type HasMethodImpl(...);
    public:
        static constexpr bool Value = decltype(HasMethodImpl<T>(nullptr))::value;
    };

    template<typename T>
    class THasSmallSwapMethod {
        template<typename C>
        static std::true_type HasMethodImpl(decltype(&C::swap)*);
        template<typename C>
        static std::false_type HasMethodImpl(...);
    public:
        static constexpr bool Value = decltype(HasMethodImpl<T>(nullptr))::value;
    };

#define TRACKABLE_WRAP_METHOD(METHOD)                                                              \
        template<typename... TArgs>                                                                \
        decltype(std::declval<TBase>().METHOD(std::declval<TArgs>()...)) METHOD(TArgs&&... args) { \
            auto tracker = static_cast<TDerived&>(*this).TTrackableBase<TDerived>::SpawnTracker(); \
            return TBase::METHOD(std::forward<TArgs>(args)...);                                    \
        }

#define WRAP_METHOD(METHOD, MODIFIERS)                                                                                 \
        template<typename... TArgs>                                                                                    \
        decltype(std::declval<MODIFIERS TBase>().METHOD(std::declval<TArgs>()...)) METHOD(TArgs&&... args) MODIFIERS { \
            return TBase::METHOD(std::forward<TArgs>(args)...);                                                        \
        }

#define WRAP_METHOD_BOTH(METHOD) \
        WRAP_METHOD(METHOD,) \
        WRAP_METHOD(METHOD, const)

#define WRAP_STL_ITERATORS() \
        WRAP_METHOD_BOTH(begin) \
        WRAP_METHOD(cbegin, const) \
        WRAP_METHOD_BOTH(end) \
        WRAP_METHOD(cend, const) \
        WRAP_METHOD_BOTH(rbegin) \
        WRAP_METHOD(crbegin, const) \
        WRAP_METHOD_BOTH(rend) \
        WRAP_METHOD(crend, const)

    template<typename TContainer, typename TDerived>
    struct TTrackerWrapperBase
        : protected TContainer
    {
        using TContainer::TContainer;
    };

    template<typename TBase, typename TDerived>
    struct TTrackerWrapper;

    template<typename T, typename A, typename TDerived>
    struct TTrackerWrapper<TDeque<T, A>, TDerived>
        : protected TTrackerWrapperBase<TDeque<T, A>, TDerived>
    {
        using TBase = TDeque<T, A>;
        using TWrapperBase = TTrackerWrapperBase<TDeque<T, A>, TDerived>;
        using TWrapperBase::TWrapperBase;

        size_t GetCapacityImpl() const {
            return TBase::size() * sizeof(T);
        }

        // std::deque methods
        TRACKABLE_WRAP_METHOD(assign)
        TRACKABLE_WRAP_METHOD(clear)
        TRACKABLE_WRAP_METHOD(insert)
        TRACKABLE_WRAP_METHOD(emplace)
        TRACKABLE_WRAP_METHOD(erase)
        TRACKABLE_WRAP_METHOD(push_front)
        TRACKABLE_WRAP_METHOD(push_back)
        TRACKABLE_WRAP_METHOD(emplace_front)
        TRACKABLE_WRAP_METHOD(emplace_back)
        TRACKABLE_WRAP_METHOD(pop_front)
        TRACKABLE_WRAP_METHOD(pop_back)

        // non-modifying methods
        WRAP_METHOD(get_allocator, const)
        WRAP_METHOD_BOTH(at)
        WRAP_METHOD_BOTH(operator[])
        WRAP_METHOD_BOTH(front)
        WRAP_METHOD_BOTH(back)
        WRAP_STL_ITERATORS()
        WRAP_METHOD(empty, const)
        WRAP_METHOD(size, const)
    };

    template<typename T, typename A, typename TDerived>
    struct TTrackerWrapper<TVector<T, A>, TDerived>
        : protected TTrackerWrapperBase<TVector<T, A>, TDerived>
    {
        using TBase = TVector<T, A>;
        using TWrapperBase = TTrackerWrapperBase<TVector<T, A>, TDerived>;
        using TWrapperBase::TWrapperBase;

        size_t GetCapacityImpl() const {
            return TBase::capacity() * sizeof(T);
        }

        // ya-specific TVector methods
        TRACKABLE_WRAP_METHOD(crop)

        // std::vector methods
        TRACKABLE_WRAP_METHOD(assign)
        TRACKABLE_WRAP_METHOD(reserve)
        TRACKABLE_WRAP_METHOD(shrink_to_fit)
        TRACKABLE_WRAP_METHOD(clear)
        TRACKABLE_WRAP_METHOD(insert)
        TRACKABLE_WRAP_METHOD(emplace)
        TRACKABLE_WRAP_METHOD(erase)
        TRACKABLE_WRAP_METHOD(push_back)
        TRACKABLE_WRAP_METHOD(emplace_back)
        TRACKABLE_WRAP_METHOD(pop_back)
        TRACKABLE_WRAP_METHOD(resize)

        // non-modifying methods
        WRAP_METHOD(get_allocator, const)
        WRAP_METHOD_BOTH(at)
        WRAP_METHOD_BOTH(operator[])
        WRAP_METHOD_BOTH(front)
        WRAP_METHOD_BOTH(back)
        WRAP_METHOD_BOTH(data)
        WRAP_STL_ITERATORS()
        WRAP_METHOD(empty, const)
        WRAP_METHOD(size, const)
        WRAP_METHOD(max_size, const)
        WRAP_METHOD(capacity, const)
    };

    template<typename T, typename A, typename TDerived>
    struct TTrackerWrapper<TList<T, A>, TDerived>
        : protected TTrackerWrapperBase<TList<T, A>, TDerived>
    {
        using TBase = TList<T, A>;
        using TWrapperBase = TTrackerWrapperBase<TList<T, A>, TDerived>;
        using TWrapperBase::TWrapperBase;

        size_t GetCapacityImpl() const {
            size_t itemSize = sizeof(T) + 2 * sizeof(void *);
            itemSize = (itemSize + sizeof(void *) - 1) & ~(sizeof(void *) - 1);
            return TBase::size() * itemSize;
        }

        TRACKABLE_WRAP_METHOD(assign)
        TRACKABLE_WRAP_METHOD(clear)
        TRACKABLE_WRAP_METHOD(insert)
        TRACKABLE_WRAP_METHOD(emplace)
        TRACKABLE_WRAP_METHOD(erase)
        TRACKABLE_WRAP_METHOD(push_back)
        TRACKABLE_WRAP_METHOD(emplace_back)
        TRACKABLE_WRAP_METHOD(pop_back)
        TRACKABLE_WRAP_METHOD(push_front)
        TRACKABLE_WRAP_METHOD(emplace_front)
        TRACKABLE_WRAP_METHOD(pop_front)
        TRACKABLE_WRAP_METHOD(resize)
        TRACKABLE_WRAP_METHOD(remove)
        TRACKABLE_WRAP_METHOD(remove_if)
        TRACKABLE_WRAP_METHOD(unique)

        template<typename... TArgs>
        void splice(typename TBase::const_iterator pos, TDerived& other, TArgs&&... args) {
            auto tracker = static_cast<TDerived&>(*this).TTrackableBase<TDerived>::SpawnTracker();
            auto otherTracker = other.TTrackableBase<TDerived>::SpawnTracker();
            TBase::splice(pos, other, std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void splice(typename TBase::const_iterator pos, TDerived&& other, TArgs&&... args) {
            splice(pos, other, std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void merge(TDerived& other, TArgs&&... args) {
            auto tracker = static_cast<TDerived&>(*this).TTrackableBase<TDerived>::SpawnTracker();
            auto otherTracker = other.TTrackableBase<TDerived>::SpawnTracker();
            TBase::merge(other, std::forward<TArgs>(args)...);
        }

        template<typename... TArgs>
        void merge(TDerived&& other, TArgs&&... args) {
            merge(other, std::forward<TArgs>(args)...);
        }

        // non-size-modifying methods
        WRAP_METHOD(get_allocator, const)
        WRAP_METHOD_BOTH(front)
        WRAP_METHOD_BOTH(back)
        WRAP_STL_ITERATORS()
        WRAP_METHOD(empty, const)
        WRAP_METHOD(size, const)
        WRAP_METHOD(max_size, const)
        WRAP_METHOD(reverse,)
        WRAP_METHOD(sort,)

        using typename TBase::iterator;
        using typename TBase::reverse_iterator;
        using typename TBase::const_iterator;
        using typename TBase::const_reverse_iterator;
    };

    template<typename TDerived>
    struct TTrackerWrapper<TBuffer, TDerived>
        : protected TTrackerWrapperBase<TBuffer, TDerived>
    {
        using TBase = TBuffer;
        using TWrapperBase = TTrackerWrapperBase<TBuffer, TDerived>;
        using TWrapperBase::TWrapperBase;

        size_t GetCapacityImpl() const {
            return TBase::Capacity();
        }

        TRACKABLE_WRAP_METHOD(Assign)
        TRACKABLE_WRAP_METHOD(Append)
        TRACKABLE_WRAP_METHOD(ChopHead)
        TRACKABLE_WRAP_METHOD(Proceed)
        TRACKABLE_WRAP_METHOD(Advance)
        TRACKABLE_WRAP_METHOD(Reserve)
        TRACKABLE_WRAP_METHOD(ShrinkToFit)
        TRACKABLE_WRAP_METHOD(Resize)
        TRACKABLE_WRAP_METHOD(AlignUp)

        // non-capacity-modifying methods
        WRAP_METHOD(Clear,)
        WRAP_METHOD(EraseBack,)
        WRAP_METHOD_BOTH(Data)
        WRAP_METHOD_BOTH(Pos)
        WRAP_METHOD(Size, const)
        WRAP_METHOD(Empty, const)
        WRAP_METHOD(Avail, const)
        WRAP_METHOD(Capacity, const)
        WRAP_METHOD_BOTH(Begin)
        WRAP_METHOD_BOTH(End)
    };

    template<typename TDerived>
    struct TTrackerWrapper<TString, TDerived>
        : protected TTrackerWrapperBase<TString, TDerived>
    {
        using TBase = TString;
        using TWrapperBase = TTrackerWrapperBase<TString, TDerived>;
        using TWrapperBase::TWrapperBase;

        size_t GetCapacityImpl() const {
            return TBase::capacity();
        }

        TRACKABLE_WRAP_METHOD(reserve)
        TRACKABLE_WRAP_METHOD(resize)
        TRACKABLE_WRAP_METHOD(clear)
        TRACKABLE_WRAP_METHOD(assign)
        TRACKABLE_WRAP_METHOD(AssignNoAlias)
        TRACKABLE_WRAP_METHOD(append)
        TRACKABLE_WRAP_METHOD(ReserveAndResize)
        TRACKABLE_WRAP_METHOD(push_back)
        TRACKABLE_WRAP_METHOD(prepend)
        TRACKABLE_WRAP_METHOD(insert)
        TRACKABLE_WRAP_METHOD(remove)
        TRACKABLE_WRAP_METHOD(erase)
        TRACKABLE_WRAP_METHOD(pop_back)
        TRACKABLE_WRAP_METHOD(operator +=)
        TRACKABLE_WRAP_METHOD(operator *=)

        // non-capacity-modifying methods
        WRAP_METHOD(off, const)
        WRAP_METHOD(IterOff, const)
        WRAP_METHOD(c_str, const)
        WRAP_METHOD(begin, const)
        WRAP_METHOD(cbegin, const)
        WRAP_METHOD(end, const)
        WRAP_METHOD(cend, const)
        WRAP_METHOD(back, const)
        WRAP_METHOD(size, const)
        WRAP_METHOD(capacity, const)
        WRAP_METHOD(is_null, const)
        WRAP_METHOD(empty, const)
        WRAP_METHOD(data, const)
        WRAP_METHOD(compare, const)
        WRAP_METHOD(equal, const)
        WRAP_METHOD(StartsWith, const)
        WRAP_METHOD(EndsWith, const)
        WRAP_METHOD(at, const)
        WRAP_METHOD(operator[], const)
        WRAP_METHOD(find, const)
        WRAP_METHOD(rfind, const)
        WRAP_METHOD(Contains, const)
        WRAP_METHOD(find_first_of, const)
        WRAP_METHOD(find_first_not_of, const)
        WRAP_METHOD(find_last_of, const)
        WRAP_METHOD(copy, const)
        WRAP_METHOD(strcpy, const)
        WRAP_METHOD(substr, const)

        decltype(std::declval<const TString>().data()) operator~() const {
            return TBase::data();
        }
    };

    template<typename TBase>
    class TTrackable
        : public TTrackerWrapper<TBase, TTrackable<TBase>>
        , public TTrackableBase<TTrackable<TBase>>
    {
    public:
        template<typename... TArgs>
        TTrackable(TMemoryConsumer&& consumer, TArgs&&... args)
            : TTrackerWrapper<TBase, TTrackable<TBase>>(std::forward<TArgs>(args)...)
            , TTrackableBase<TTrackable<TBase>>(std::forward<TMemoryConsumer>(consumer))
        {}

        // copy constructor
        TTrackable(const TTrackable& other)
            : TTrackerWrapper<TBase, TTrackable<TBase>>(other)
            , TTrackableBase<TTrackable<TBase>>(other)
        {}

        // move constructor
        TTrackable(TTrackable&& other)
            : TTrackerWrapper<TBase, TTrackable<TBase>>()
            , TTrackableBase<TTrackable<TBase>>(other)
        {
            *this = std::move(other);
        }

        // copy assign
        TTrackable& operator =(const TTrackable& other) {
            auto tracker = TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            static_cast<TBase&>(*this) = other;
            return *this;
        }

        // move assign
        TTrackable& operator =(TTrackable&& other) {
            auto tracker = TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            auto otherTracker = other.TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            static_cast<TBase&>(*this) = std::move(other);
            return *this;
        }

        // const cast operator
        const TBase& GetBaseConstRef() const {
            return *this;
        }

        // reset - completely free memory used by this object
        void Reset() {
            auto tracker = TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            TBase empty;
            DoSwap<TBase>(empty, *this);
        }

        void Swap(TTrackable& other) {
            auto tracker = TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            auto otherTracker = other.TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            TBase::Swap(other);
        }

        void swap(TTrackable& other) {
            auto tracker = TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            auto otherTracker = other.TTrackableBase<TTrackable<TBase>>::SpawnTracker();
            TBase::swap(other);
        }
    };

    template<typename T, typename A = typename TDeque<T>::allocator_type>
    using TTrackableDeque = TTrackable<TDeque<T, A>>;
    template<typename T, typename A = typename TVector<T>::allocator_type>
    using TTrackableVector = TTrackable<TVector<T, A>>;
    template<typename T, typename A = typename TList<T>::allocator_type>
    using TTrackableList = TTrackable<TList<T, A>>;
    using TTrackableBuffer = TTrackable<TBuffer>;
    using TTrackableString = TTrackable<TString>;

} // NKikimr

template<typename TBase, std::enable_if_t<NKikimr::THasCapitalSwapMethod<TBase>::Value>* = nullptr>
void DoSwap(NKikimr::TTrackable<TBase>& first, NKikimr::TTrackable<TBase>& second) {
    first.Swap(second);
}

template<typename TBase, std::enable_if_t<NKikimr::THasSmallSwapMethod<TBase>::Value>* = nullptr>
void DoSwap(NKikimr::TTrackable<TBase>& first, NKikimr::TTrackable<TBase>& second) {
    first.swap(second);
}

namespace std {
    template<typename TBase>
    inline void swap(NKikimr::TTrackable<TBase>& first, NKikimr::TTrackable<TBase>& second) {
        DoSwap(first, second);
    }
} // std
