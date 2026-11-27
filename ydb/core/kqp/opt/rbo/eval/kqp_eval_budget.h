#pragma once

#include "kqp_eval.h"

#include <util/generic/array_ref.h>

#include <algorithm>
#include <ranges>

namespace NKikimr::NKqp::NEval::NDetail {

// Internal guards unwind to the single public Evaluate boundary. Operator
// refusals remain TResult values; no partial value escapes a resource failure.
struct TBudgetExceeded {
    EResource Resource;
};

class TBudget {
public:
    explicit TBudget(TLimits limits)
        : Limits_(limits)
        , Memory_(std::make_shared<TAllocationBudget>(limits.Memory))
    {
        Limits_.Depth = std::min<size_t>(limits.Depth, 64);
        Limits_.Width = std::min<size_t>(limits.Width, 64);
        Limits_.Ranges = std::min<size_t>(limits.Ranges, 4096);
        Limits_.LiteralBytes = std::min<size_t>(limits.LiteralBytes, 64 * 1024);
    }

    void Step() {
        Consume(1, Limits_.Work, Usage_.Work, EResource::Work);
    }

    void Visit(const NYql::TExprNode& node, size_t depth) {
        Check(depth, Limits_.Depth, EResource::Depth);
        Step();
        Scan(node.Content().size());
    }

    // Charge the entire input of a variable-length primitive, even if it exits
    // early. No byte-by-byte metering or caller-supplied complexity formula.
    void Scan(size_t bytes) {
        Consume(bytes, Limits_.Bytes, Usage_.Bytes, EResource::Bytes);
    }

    void Literal(size_t bytes) {
        Check(bytes, Limits_.LiteralBytes, EResource::LiteralBytes);
        Scan(bytes);
    }

    void Width(size_t count) const {
        Check(count, Limits_.Width, EResource::Width);
    }

    void Ranges(size_t count) const {
        Check(count, Limits_.Ranges, EResource::Ranges);
    }

    // Iterating variable-sized input consumes work automatically. Small loops
    // over a validated key width (at most 64) need no per-component checks.
    template <class TIterator>
    class TIteration {
    public:
        struct TIteratorWithBudget {
            TIterator It;
            TBudget* Budget;

            decltype(auto) operator*() const {
                // Charge before the body runs, including iterations that break
                // or continue without producing an output value.
                Budget->Step();
                return *It;
            }

            void operator++() {
                ++It;
            }

            bool operator!=(const TIteratorWithBudget& other) const {
                return It != other.It;
            }
        };

        TIterator First;
        TIterator Last;
        TBudget* Budget;

        auto begin() const {
            return TIteratorWithBudget{First, Budget};
        }

        auto end() const {
            return TIteratorWithBudget{Last, Budget};
        }
    };

    template <class TRange>
    auto Each(TRange& range) {
        return TIteration<decltype(std::begin(range))>{std::begin(range), std::end(range), this};
    }

    // AST Children() returns this borrowed view by value. Owning temporary
    // containers deliberately cannot bind to Each: their iterators would dangle.
    template <class T>
    auto Each(TArrayRef<T> range) {
        return TIteration<decltype(range.begin())>{range.begin(), range.end(), this};
    }

    auto Indices(size_t first, size_t last) {
        // An empty input may have first > last (e.g. adjacent pairs start at 1).
        auto range = std::views::iota(first, std::max(first, last));
        return Each(range);   // iota iterators own their index; they do not borrow the view.
    }

    auto Indices(size_t count) {
        return Indices(0, count);
    }

    template <class T>
    TAllocator<T> Allocator() const {
        return TAllocator<T>(Memory_);
    }

    template <class T>
    TVector<T> Vector() const {
        return TVector<T>(Allocator<T>());
    }

    template <class T, class... TArgs>
    std::shared_ptr<T> Make(TArgs&&... args) const {
        return std::allocate_shared<T>(Allocator<T>(), std::forward<TArgs>(args)...);
    }

    std::shared_ptr<const TString> String(TStringBuf prefix, TStringBuf suffix = {}) {
        Check(prefix.size(), Limits_.LiteralBytes, EResource::LiteralBytes);
        Check(suffix.size(), Limits_.LiteralBytes - prefix.size(), EResource::LiteralBytes);
        Literal(prefix.size() + suffix.size());
        auto result = Make<TString>(Allocator<char>());
        result->reserve(prefix.size() + suffix.size());
        if (!prefix.empty()) {
            result->append(prefix.data(), prefix.size());
        }
        if (!suffix.empty()) {
            result->append(suffix.data(), suffix.size());
        }
        return result;
    }

    TUsage Usage() const {
        return {.Work = Usage_.Work, .Bytes = Usage_.Bytes, .Memory = Memory_->Used};
    }

private:
    static void Check(size_t count, size_t maximum, EResource resource) {
        if (count > maximum) {
            throw TBudgetExceeded{resource};
        }
    }

    static void Consume(size_t amount, size_t maximum, size_t& used, EResource resource) {
        // Maintain used <= maximum; test the remaining allowance before adding.
        Check(amount, maximum - used, resource);
        used += amount;
    }

    TLimits Limits_;
    TUsage Usage_;
    std::shared_ptr<TAllocationBudget> Memory_;
};

}   // namespace NKikimr::NKqp::NEval::NDetail
