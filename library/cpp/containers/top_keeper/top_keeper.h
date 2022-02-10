#pragma once

#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/str_stl.h>

template <class T, class TComparator = TGreater<T>, bool sort = true, class Alloc = std::allocator<T>>
class TTopKeeper {
private:
    class TVectorWithMin {
    private:
        TVector<T, Alloc> Internal;
        size_t HalfMaxSize;
        TComparator Comparer;
        size_t MinElementIndex;

    private:
        void Reserve() {
            Internal.reserve(2 * HalfMaxSize);
        }

        template <class UT>
        bool Insert(UT&& value) noexcept {
            if (Y_UNLIKELY(0 == HalfMaxSize)) {
                return false;
            }

            if (Internal.size() < HalfMaxSize) {
                if (Internal.empty() || Comparer(Internal[MinElementIndex], value)) {
                    MinElementIndex = Internal.size();
                    Internal.push_back(std::forward<UT>(value));
                    return true;
                }
            } else if (!Comparer(value, Internal[MinElementIndex])) {
                return false;
            }

            Internal.push_back(std::forward<UT>(value));

            if (Internal.size() == (HalfMaxSize << 1)) {
                Partition();
            }

            return true;
        }

    public:
        using value_type = T;

        TVectorWithMin(const size_t halfMaxSize, const TComparator& comp)
            : HalfMaxSize(halfMaxSize)
            , Comparer(comp)
        {
            Reserve();
        }

        template <class TAllocParam>
        TVectorWithMin(const size_t halfMaxSize, const TComparator& comp, TAllocParam&& param)
            : Internal(std::forward<TAllocParam>(param))
            , HalfMaxSize(halfMaxSize)
            , Comparer(comp)
        {
            Reserve();
        }

        void SortAccending() {
            Sort(Internal.begin(), Internal.end(), Comparer);
        }

        void Partition() {
            if (Y_UNLIKELY(HalfMaxSize == 0)) {
                return;
            }
            if (Y_LIKELY(Internal.size() >= HalfMaxSize)) {
                NthElement(Internal.begin(), Internal.begin() + HalfMaxSize - 1, Internal.end(), Comparer);
                Internal.erase(Internal.begin() + HalfMaxSize, Internal.end());

                //we should update MinElementIndex cause we just altered Internal
                MinElementIndex = HalfMaxSize - 1;
            }
        }

        bool Push(const T& value) {
            return Insert(value);
        }

        bool Push(T&& value) {
            return Insert(std::move(value));
        }

        template <class... TArgs>
        bool Emplace(TArgs&&... args) {
            return Insert(T(std::forward<TArgs>(args)...)); // TODO: make it "real" emplace, not that fake one
        }

        void SetMaxSize(size_t newHalfMaxSize) {
            HalfMaxSize = newHalfMaxSize;
            Reserve();
            Partition();
        }

        size_t GetSize() const {
            return Internal.size();
        }

        const auto& GetInternal() const {
            return Internal;
        }

        auto Extract() {
            using std::swap;

            decltype(Internal) values;
            swap(Internal, values);
            Reset();
            return values;
        }

        const T& Back() const {
            return Internal.back();
        }

        void Pop() {
            Internal.pop_back();
        }

        void Reset() {
            Internal.clear();
            //MinElementIndex will reset itself when we start adding new values
        }
    };

    void CheckNotFinalized() {
        Y_ENSURE(!Finalized, "Cannot insert after finalizing (Pop() / GetNext() / Finalize())! "
                             "Use TLimitedHeap for this scenario");
    }

    size_t MaxSize;
    const TComparator Comparer;
    TVectorWithMin Internal;
    bool Finalized;

public:
    TTopKeeper()
        : MaxSize(0)
        , Comparer()
        , Internal(0, Comparer)
        , Finalized(false)
    {
    }

    TTopKeeper(size_t maxSize, const TComparator& comp = TComparator())
        : MaxSize(maxSize)
        , Comparer(comp)
        , Internal(maxSize, comp)
        , Finalized(false)
    {
    }

    template <class TAllocParam>
    TTopKeeper(size_t maxSize, const TComparator& comp, TAllocParam&& param)
        : MaxSize(maxSize)
        , Comparer(comp)
        , Internal(maxSize, comp, std::forward<TAllocParam>(param))
        , Finalized(false)
    {
    }

    void Finalize() {
        if (Y_LIKELY(Finalized)) {
            return;
        }
        Internal.Partition();
        if (sort) {
            Internal.SortAccending();
        }
        Finalized = true;
    }

    const T& GetNext() {
        Y_ENSURE(!IsEmpty(), "Trying GetNext from empty heap!");
        Finalize();
        return Internal.Back();
    }

    void Pop() {
        Y_ENSURE(!IsEmpty(), "Trying Pop from empty heap!");
        Finalize();
        Internal.Pop();
        if (IsEmpty()) {
            Reset();
        }
    }

    T ExtractOne() {
        Y_ENSURE(!IsEmpty(), "Trying ExtractOne from empty heap!");
        Finalize();
        auto value = std::move(Internal.Back());
        Internal.Pop();
        if (IsEmpty()) {
            Reset();
        }
        return value;
    }

    auto Extract() {
        Finalize();
        return Internal.Extract();
    }

    bool Insert(const T& value) {
        CheckNotFinalized();
        return Internal.Push(value);
    }

    bool Insert(T&& value) {
        CheckNotFinalized();
        return Internal.Push(std::move(value));
    }

    template <class... TArgs>
    bool Emplace(TArgs&&... args) {
        CheckNotFinalized();
        return Internal.Emplace(std::forward<TArgs>(args)...);
    }

    const auto& GetInternal() {
        Finalize();
        return Internal.GetInternal();
    }

    bool IsEmpty() const {
        return Internal.GetSize() == 0;
    }

    size_t GetSize() const {
        return Min(Internal.GetSize(), MaxSize);
    }

    size_t GetMaxSize() const {
        return MaxSize;
    }

    void SetMaxSize(size_t newMaxSize) {
        Y_ENSURE(!Finalized, "Cannot resize after finalizing (Pop() / GetNext() / Finalize())! "
                             "Use TLimitedHeap for this scenario");
        MaxSize = newMaxSize;
        Internal.SetMaxSize(newMaxSize);
    }

    void Reset() {
        Internal.Reset();
        Finalized = false;
    }
};
