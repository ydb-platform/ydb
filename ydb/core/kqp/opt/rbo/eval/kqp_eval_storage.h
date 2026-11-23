#pragma once

#include <memory>
#include <new>
#include <string>
#include <vector>

namespace NKikimr::NKqp::NEval::NDetail {

// Shared by all evaluator allocations, including temporaries and shared_ptr
// control blocks. It owns no allocations itself, so escaping values cannot form
// a cycle. Releases do not refund the allowance: allocation churn is bounded too.
struct TAllocationBudget {
    explicit TAllocationBudget(size_t limit)
        : Limit(limit)
    {
    }

    const size_t Limit;
    size_t Used = 0;
};

template <class T>
class TAllocator {
public:
    using value_type = T;
    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;

    TAllocator() noexcept = default;

    explicit TAllocator(std::shared_ptr<TAllocationBudget> budget) noexcept
        : Budget_(std::move(budget))
    {
    }

    template <class U>
    TAllocator(const TAllocator<U>& other) noexcept
        : Budget_(other.Budget_)
    {
    }

    T* allocate(size_t count) {
        // An unbound container must fail closed, never allocate outside a session.
        // Divide the remaining allowance first so count * sizeof(T) cannot overflow.
        if (!Budget_ || count > (Budget_->Limit - Budget_->Used) / sizeof(T)) {
            throw std::bad_alloc();
        }
        Budget_->Used += count * sizeof(T);
        return std::allocator<T>{}.allocate(count);
    }

    void deallocate(T* data, size_t count) noexcept {
        std::allocator<T>{}.deallocate(data, count);
    }

    template <class U>
    bool operator==(const TAllocator<U>& other) const noexcept {
        return Budget_ == other.Budget_;
    }

private:
    template <class>
    friend class TAllocator;
    std::shared_ptr<TAllocationBudget> Budget_;
};

template <class T>
using TVector = std::vector<T, TAllocator<T>>;
using TString = std::basic_string<char, std::char_traits<char>, TAllocator<char>>;

}   // namespace NKikimr::NKqp::NEval::NDetail
