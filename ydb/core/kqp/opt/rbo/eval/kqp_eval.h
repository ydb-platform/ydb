#pragma once

#include "kqp_eval_storage.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <contrib/restricted/abseil-cpp/absl/container/inlined_vector.h>

#include <memory>
#include <optional>
#include <variant>

namespace NKikimr::NKqp::NEval {

// Expected refusals are values. No query issues, MiniKQL graph execution or UDF calls.
enum class EError {
    Unsupported,   // Valid forms outside the implemented subset.
    NonConstant,   // Evaluation depends on runtime input or nondeterminism.
    BudgetExceeded,
    InvalidInput,   // A recognized form has malformed operands or literal data.
    EvaluationError,
};

enum class EResource {
    None,
    Work,
    Bytes,
    Memory,
    Depth,
    Ranges,
    Width,
    LiteralBytes,
};

struct TError {
    EError Code;
    EResource Resource = EResource::None;
};

template <class T>
class TResult {
public:
    TResult(T value)
        : Data_(std::move(value))
    {
    }

    TResult(TError error)
        : Data_(error)
    {
    }

    const T* GetValue() const {
        return std::get_if<T>(&Data_);
    }

    T* GetValue() {
        return std::get_if<T>(&Data_);
    }

    const TError* GetError() const {
        return std::get_if<TError>(&Data_);
    }

private:
    std::variant<T, TError> Data_;
};

// Scalar payloads use MiniKQL's inline representation. Strings are owned, immutable
// bytes: no MiniKQL allocator, lazy boxed values, execution context or UDF calls.
struct TScalar {
    bool Null = false;
    NYql::NUdf::EDataSlot Slot = NYql::NUdf::EDataSlot::Bool;
    NYql::NUdf::TUnboxedValuePod Data;
    std::shared_ptr<const NDetail::TString> Bytes;

    TStringBuf Text() const {
        return Bytes ? TStringBuf(Bytes->data(), Bytes->size()) : TStringBuf();
    }
};

struct TComponent {
    i32 Inf = 0;   // -1 / +1 for infinities; 0 means Value is a finite key (possibly NULL).
    TScalar Value;
};

struct TBoundary {
    explicit TBoundary(NDetail::TAllocator<TComponent> allocator = {})
        : Components(allocator)
    {
    }

    TBoundary(const TBoundary&) = default;
    TBoundary(TBoundary&&) = default;

    TBoundary& operator=(TBoundary other) noexcept {
        // Abseil assignment does not propagate stateful allocators; swap does.
        Components.swap(other.Components);
        Included = other.Included;
        return *this;
    }

    absl::InlinedVector<TComponent, 2, NDetail::TAllocator<TComponent>> Components;
    i32 Included = 0;   // MiniKQL: -1 left inclusive, +1 right inclusive, 0 exclusive.

    bool HasPrefix() const {
        return !Components.empty() && !Components.front().Inf;
    }
};

struct TRange {
    TBoundary Left;
    TBoundary Right;
};

struct TRanges {
    NDetail::TVector<TRange> Items;
    size_t Width = 0;
    // Finalized range values cannot be fed back into internal range operators.
    bool Finalized = false;
};

// Generic, immutable result. New value domains can be added without changing the
// evaluator entry point. Ranges retain their exact internal/finalized distinction;
// this is not an arbitrary YQL-value serializer or an AST-rewriting API.
class TValue {
public:
    explicit TValue(TScalar value)
        : Data_(std::move(value))
    {
    }

    explicit TValue(TRanges value)
        : Data_(std::move(value))
    {
    }

    const TScalar* GetScalar() const {
        return std::get_if<TScalar>(&Data_);
    }

    const TRanges* GetRanges() const {
        return std::get_if<TRanges>(&Data_);
    }

private:
    std::variant<TScalar, TRanges> Data_;
};

using TValuePtr = std::shared_ptr<const TValue>;

struct TLimits {
    // Coarse safety fuel for visits, variable-sized iteration and range
    // primitives. These counters are not execution-time estimates.
    // Leave room for 256-point inputs, but stop large operator chains early.
    size_t Work = 8 * 1024;
    size_t Bytes = 256 * 1024;   // Whole inputs to string/byte primitives, including early exits.

    // Cumulative allocator requests, including temporary storage and shared_ptr
    // control blocks. No refunds. Excludes fixed session setup, the caller's AST
    // and the system allocator's metadata; this is not a total RSS limit.
    // Also bounds repeated copies/comparisons of wide, cached range operands.
    size_t Memory = 2 * 1024 * 1024;

    // These shape limits also have internal hard caps.
    size_t Depth = 64;
    size_t Ranges = 256;
    size_t Width = 64;
    size_t LiteralBytes = 64 * 1024;
};

struct TUsage {
    size_t Work = 0;
    size_t Bytes = 0;
    size_t Memory = 0;
};

// One session per compilation/plan, confined to its owning thread. The input's
// unfrozen TExprContext must outlive the session (node pointers do not own its
// arena). Expressions must not be mutated in place while the session is alive:
// rewrites must create fresh nodes or start a new session. Cached input nodes are
// retained. Results own their payloads and survive both context and session. All
// attempts share the allowance, including cache lookups and failed attempts.
// Construction does not allocate; Evaluate initializes storage under its error
// boundary. GetUsage returns zero counters until initialization succeeds.
class TSession {
public:
    explicit TSession(TLimits limits = {}) noexcept;
    ~TSession();
    TSession(const TSession&) = delete;
    TSession& operator=(const TSession&) = delete;

    TResult<TValuePtr> Evaluate(const NYql::TExprNode::TPtr& expression);
    TUsage GetUsage() const;

private:
    class TImpl;
    TLimits Limits_;
    std::unique_ptr<TImpl> Impl_;
};

}   // namespace NKikimr::NKqp::NEval
