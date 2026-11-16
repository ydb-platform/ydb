#include "kqp_eval.h"
#include "kqp_eval_budget.h"

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/utils/utf8.h>
#include <util/charset/utf8.h>
#include <util/string/cast.h>

#include <algorithm>
#include <limits>
#include <unordered_map>

// Bounded evaluation of a supported subset of YQL expression nodes.
//
// TSession is the public boundary: it returns either an owned, immutable TValue
// or an explicit refusal. TInterpreter::Evaluate memoizes successful nodes;
// Run dispatches callables to routines that decode and validate their operands.
// Handlers return typed TResult values; MakeValue boxes successes for the cache
// and public API. Error propagation uses ordinary checks and early returns.
// Conditional evaluation visits only the selected branch. No MiniKQL execution
// graph, arbitrary lambda, or UDF is run here.
//
// Scalars use inline MiniKQL payloads and owned strings. Range operators work on
// typed boundaries with separate NULL, infinity, and inclusion markers. Operand
// copies cached ranges before normalizing them, so operators cannot mutate a
// cached value. Normalization sorts and removes empty/duplicate intervals;
// merging overlaps is a separate operation belonging to RangeUnion.
//
// Budgeting happens during evaluation: visits and bounded iteration consume
// work, string primitives charge their inputs, and value builders cap shapes.
// Allocators enforce one cumulative memory allowance across values, cache, and
// scratch storage. Resource guards unwind to TSession::Evaluate, which converts
// them to BudgetExceeded; semantic refusals propagate through TResult. Session
// storage is initialized lazily inside that same error boundary.
// New handlers should reuse these primitives and explicitly decline semantics
// they cannot reproduce. Explain formats the resulting values separately.

namespace NKikimr::NKqp::NEval {
namespace {
using namespace NYql;
using NDecimal::TInt128;
using NDetail::TBudget;
using NUdf::EDataSlot;

bool IsInteger(EDataSlot slot) {
    return slot >= EDataSlot::Int8 && slot <= EDataSlot::Uint64;
}

// These are semantic bounds, not merely storage bounds: date/time encodings
// reserve values that must never appear in a successfully evaluated literal.
std::optional<std::pair<TInt128, TInt128>> IntegerLimits(EDataSlot slot) {
    switch (slot) {
        case EDataSlot::Int8:
            return {{Min<i8>(), Max<i8>()}};
        case EDataSlot::Uint8:
            return {{Min<ui8>(), Max<ui8>()}};
        case EDataSlot::Int16:
            return {{Min<i16>(), Max<i16>()}};
        case EDataSlot::Uint16:
            return {{Min<ui16>(), Max<ui16>()}};
        case EDataSlot::Int32:
            return {{Min<i32>(), Max<i32>()}};
        case EDataSlot::Uint32:
            return {{Min<ui32>(), Max<ui32>()}};
        case EDataSlot::Int64:
            return {{Min<i64>(), Max<i64>()}};
        case EDataSlot::Uint64:
            return {{Min<ui64>(), Max<ui64>()}};
        case EDataSlot::Date:
            return {{0, NUdf::MAX_DATE - 1}};
        case EDataSlot::Datetime:
            return {{0, NUdf::MAX_DATETIME - 1}};
        case EDataSlot::Timestamp:
            return {{0, NUdf::MAX_TIMESTAMP - 1}};
        case EDataSlot::Date32:
            return {{NUdf::MIN_DATE32, NUdf::MAX_DATE32}};
        case EDataSlot::Datetime64:
            return {{NUdf::MIN_DATETIME64, NUdf::MAX_DATETIME64}};
        case EDataSlot::Timestamp64:
            return {{NUdf::MIN_TIMESTAMP64, NUdf::MAX_TIMESTAMP64}};
        default:
            return {};
    }
}

// Decode by the physical MiniKQL slot. Int128 can represent every supported
// signed/unsigned value and their differences without overflow.
std::optional<TInt128> Integer(const TScalar& value) {
    if (value.Null) {
        return {};
    }

    switch (value.Slot) {
        case EDataSlot::Int8:
            return value.Data.Get<i8>();
        case EDataSlot::Uint8:
            return value.Data.Get<ui8>();
        case EDataSlot::Int16:
            return value.Data.Get<i16>();
        case EDataSlot::Uint16:
        case EDataSlot::Date:
            return value.Data.Get<ui16>();
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            return value.Data.Get<i32>();
        case EDataSlot::Uint32:
        case EDataSlot::Datetime:
            return value.Data.Get<ui32>();
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            return value.Data.Get<i64>();
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            return value.Data.Get<ui64>();
        default:
            return {};
    }
}

// Callers have already checked IntegerLimits before constructing a converted
// value or the successor of an integral range endpoint.
TScalar IntegerValue(EDataSlot slot, TInt128 value) {
    TScalar result{.Slot = slot};
    switch (slot) {
        case EDataSlot::Int8:
            result.Data = NUdf::TUnboxedValuePod(i8(value));
            break;
        case EDataSlot::Uint8:
            result.Data = NUdf::TUnboxedValuePod(ui8(value));
            break;
        case EDataSlot::Int16:
            result.Data = NUdf::TUnboxedValuePod(i16(value));
            break;
        case EDataSlot::Uint16:
        case EDataSlot::Date:
            result.Data = NUdf::TUnboxedValuePod(ui16(value));
            break;
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            result.Data = NUdf::TUnboxedValuePod(i32(value));
            break;
        case EDataSlot::Uint32:
        case EDataSlot::Datetime:
            result.Data = NUdf::TUnboxedValuePod(ui32(value));
            break;
        case EDataSlot::Int64:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
            result.Data = NUdf::TUnboxedValuePod(i64(value));
            break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            result.Data = NUdf::TUnboxedValuePod(ui64(value));
            break;
        default:
            break;   // Only called after IntegerLimits succeeds.
    }
    return result;
}

template <class T>
TResult<NUdf::TUnboxedValuePod> ParseNumber(TStringBuf text) {
    T value;
    if (!TryFromString(text, value)) {
        return TError{EError::InvalidInput};
    }

    return NUdf::TUnboxedValuePod(value);
}

// Parse the physical representation first. Literal checks the narrower semantic
// bounds for date/time slots afterward; unsupported slots never reach a getter.
TResult<NUdf::TUnboxedValuePod> ParseNumericLiteral(EDataSlot slot, TStringBuf text) {
    switch (slot) {
        case EDataSlot::Bool:
            return ParseNumber<bool>(text);
        case EDataSlot::Int8:
            return ParseNumber<i8>(text);
        case EDataSlot::Uint8:
            return ParseNumber<ui8>(text);
        case EDataSlot::Int16:
            return ParseNumber<i16>(text);
        case EDataSlot::Uint16:
        case EDataSlot::Date:
            return ParseNumber<ui16>(text);
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            return ParseNumber<i32>(text);
        case EDataSlot::Uint32:
        case EDataSlot::Datetime:
            return ParseNumber<ui32>(text);
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
            return ParseNumber<i64>(text);
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            return ParseNumber<ui64>(text);
        default:
            return TError{EError::Unsupported};
    }
}

bool Supported(EDataSlot slot) {
    return IntegerLimits(slot) || slot == EDataSlot::Bool || slot == EDataSlot::String || slot == EDataSlot::Utf8 ||
           slot == EDataSlot::Uuid || slot == EDataSlot::Interval || slot == EDataSlot::Interval64;
}

i32 InfSign(bool prefix, bool included, bool left) {
    // A finite prefix continues toward -inf on the left and +inf on the right
    // when included. Excluding that prefix reverses the continuation. A wholly
    // infinite boundary has no finite prefix to exclude.
    return !prefix || included ? (left ? -1 : 1) : (left ? 1 : -1);
}

struct TKeyType {
    EDataSlot Slot;
    bool Nullable = false;
};

// Range operators use typed values. No unchecked numeric accessor is applied to
// an AST node, and no unsupported value is fed into a comparator.
class TInterpreter {
public:
    explicit TInterpreter(TBudget& budget)
        : Budget(budget)
        , Cache(budget.Allocator<TCache::value_type>())
    {
    }

    TResult<TValuePtr> Evaluate(const TExprNode::TPtr& node, size_t depth = 0);

private:
    // Dispatch and convert typed handler results into owned, immutable values.
    TResult<TValuePtr> Run(const TExprNode::TPtr& node, size_t depth);
    TResult<TValuePtr> Conditional(const TExprNode& node, size_t depth);

    template <class T>
    TResult<TValuePtr> MakeValue(TResult<T> result);

    // Decode the supported literal/type forms without executing arbitrary code.
    TResult<TScalar> Literal(const TExprNode& node, size_t depth);
    TResult<std::optional<TScalar>> NextPrefix(const TScalar& value);
    TResult<TKeyType> KeyType(const TExprNode& node, size_t depth);
    TResult<TKeyType> BoundaryKeyType(const TExprNode& node, size_t depth);

    // Callable handlers preserve the corresponding MiniKQL range semantics.
    TResult<TRanges> AsRange(const TExprNode& node, size_t depth);
    TResult<TBoundary> Boundary(const TExprNode& node, bool left, size_t depth);
    TResult<TRanges> RangeFor(const TExprNode& node, size_t depth);
    TResult<TRanges> MakeRangeFor(TStringBuf op, TScalar value, TKeyType type);
    TResult<TRanges> Empty(const TExprNode& node, size_t depth);
    TResult<TRanges> Union(const TExprNode& node, size_t depth);
    TResult<TRanges> Intersect(const TExprNode& node, size_t depth);
    TResult<TRanges> Multiply(const TExprNode& node, size_t depth);
    TResult<TRanges> MultiplyPair(const TRanges& a, const TRanges& b, ui64 limit);
    TResult<TRanges> Finalize(const TExprNode& node, size_t depth);
    TResult<TRanges> SqlIn(const TExprNode& node, size_t depth);
    TResult<NDetail::TVector<TExprNode::TPtr>> LiteralList(const TExprNode& node, size_t depth);

    // Shared range primitives validate values and use budgeted storage.
    TResult<TRanges> Operand(const TExprNode::TPtr& node, size_t depth);
    TResult<TRanges> Normalize(TRanges ranges);
    TResult<TRanges> Merge(TRanges ranges);
    TResult<int> Compare(const TScalar& a, const TScalar& b);
    TResult<int> Compare(const TBoundary& a, const TBoundary& b, bool inclusion = true);
    TResult<int> Compare(const TRange& a, const TRange& b);
    TResult<bool> IsEmpty(const TRange& range);
    void Append(TRanges& to, const TRange& range);
    void MoveAppend(TRanges& to, TRange&& range);
    TRanges NewRanges(size_t width = 0);
    TBoundary NewBoundary(size_t width, i32 included = 0);
    TBoundary SingleBoundary(TComponent component, i32 included = 0);
    TRanges Copy(const TRanges& ranges);

    // No error means the boundary shapes are valid; there is no value to return.
    std::optional<TError> Validate(const TRanges& ranges);
    TResult<ui64> Limit(const TExprNode& node, size_t depth);

    TBudget& Budget;

    // Only successful values are memoized. Failure is not intrinsic to a node:
    // in particular BudgetExceeded depends on this session's remaining budget.
    using TCache = std::unordered_map<TExprNode::TPtr, TValuePtr, THash<TExprNode::TPtr>,
        std::equal_to<TExprNode::TPtr>, NDetail::TAllocator<std::pair<const TExprNode::TPtr, TValuePtr>>>;
    TCache Cache;
};

TResult<TScalar> TInterpreter::Literal(const TExprNode& node, size_t depth) {
    Budget.Visit(node, depth);
    if (node.IsCallable("Parameter") || node.Type() == TExprNode::Argument ||
        node.IsCallable({"RandomNumber", "Random", "CurrentUtcTimestamp", "Now"})) {
        return TError{EError::NonConstant};
    }

    if (node.IsCallable("Just") && node.ChildrenSize() == 1) {
        // A range key can have one nullable layer. Do not collapse distinct
        // values of Optional<Optional<T>> into the same scalar/NULL.
        if (node.Head().IsCallable({"Just", "Nothing"})) {
            return TError{EError::Unsupported};
        }

        return Literal(node.Head(), depth + 1);
    }

    if (node.IsCallable("ToString") && node.ChildrenSize() == 1) {
        if (!node.Head().IsCallable("Utf8")) {
            return TError{EError::Unsupported};
        }

        auto valueResult = Literal(node.Head(), depth + 1);
        if (const auto* error = valueResult.GetError()) {
            return *error;
        }

        auto value = std::move(*valueResult.GetValue());
        if (value.Null || value.Slot != EDataSlot::Utf8) {
            return TError{EError::Unsupported};
        }

        value.Slot = EDataSlot::String;
        return value;
    }

    if (node.IsCallable("Nothing") && node.ChildrenSize() == 1) {
        auto typeResult = KeyType(node.Head(), depth + 1);
        if (const auto* error = typeResult.GetError()) {
            return *error;
        }

        const auto type = *typeResult.GetValue();
        if (!type.Nullable) {
            return TError{EError::InvalidInput};
        }

        return TScalar{.Null = true, .Slot = type.Slot};
    }

    if (!node.IsCallable()) {
        return TError{EError::InvalidInput};
    }

    const auto slot = NUdf::FindDataSlot(node.Content());
    if (!slot || !Supported(*slot)) {
        return TError{EError::Unsupported};
    }

    if (node.ChildrenSize() != 1 || !node.Head().IsAtom()) {
        return TError{EError::InvalidInput};
    }

    const auto text = node.Head().Content();
    Budget.Literal(text.size());
    TScalar result{.Slot = *slot};
    if (*slot == EDataSlot::String || *slot == EDataSlot::Utf8 || *slot == EDataSlot::Uuid) {
        if (*slot == EDataSlot::Uuid && text.size() != 16) {
            return TError{EError::InvalidInput};
        }

        if (*slot == EDataSlot::Utf8 && !NYql::IsUtf8(std::string_view(text.data(), text.size()))) {
            return TError{EError::InvalidInput};
        }

        result.Bytes = Budget.String(text);
        return result;
    }

    // Binary numeric atoms contain the physical value, not its decimal text.
    // Type annotation normally normalizes them; decline unnormalized inputs.
    if (node.Head().Flags() & TNodeFlags::BinaryContent) {
        return TError{EError::Unsupported};
    }

    auto dataResult = ParseNumericLiteral(*slot, text);
    if (const auto* error = dataResult.GetError()) {
        return *error;
    }

    result.Data = *dataResult.GetValue();
    // Interval validity is independent of discrete range endpoint arithmetic:
    // adding intervals to IntegerLimits would also change RangeFinalize.
    if ((*slot == EDataSlot::Interval && !NUdf::IsValidLayoutValue<NUdf::TInterval>(result.Data.Get<i64>())) ||
        (*slot == EDataSlot::Interval64 && !NUdf::IsValidLayoutValue<NUdf::TInterval64>(result.Data.Get<i64>()))) {
        return TError{EError::InvalidInput};
    }

    if (const auto limits = IntegerLimits(*slot)) {
        const auto value = *Integer(result);
        if (value < limits->first || value > limits->second) {
            return TError{EError::InvalidInput};
        }
    }
    return result;
}

// Same successor semantics as NextLexicographicString / NextValidUtf8, but
// without an unaccounted UTF-32 temporary. Inspect only the trailing code points
// and copy the surviving prefix once. Utf8 inputs have already been validated.
TResult<std::optional<TScalar>> TInterpreter::NextPrefix(const TScalar& value) {
    const auto text = value.Text();
    Budget.Scan(text.size());
    size_t end = text.size();

    while (end) {
        size_t start = end - 1;
        unsigned char encoded[4];
        size_t encodedSize = 1;
        if (value.Slot == EDataSlot::String) {
            const auto last = static_cast<unsigned char>(text[start]);
            if (last == 0xff) {
                end = start;
                continue;
            }
            encoded[0] = last + 1;
        } else {
            while (start && IsUTF8ContinuationByte(text[start])) {
                --start;
            }
            const auto* begin = reinterpret_cast<const unsigned char*>(text.data() + start);
            wchar32 rune;
            size_t length;
            if (SafeReadUTF8Char(rune, length, begin, begin + (end - start)) != RECODE_OK) {
                return TError{EError::InvalidInput};
            }

            if (rune == 0x10ffff) {
                end = start;
                continue;
            }
            rune = rune == 0xd7ff ? 0xe000 : rune + 1;
            if (SafeWriteUTF8Char(rune, encodedSize, encoded, sizeof(encoded)) != RECODE_OK) {
                return TError{EError::InvalidInput};
            }
        }
        auto bytes =
            Budget.String(text.substr(0, start), TStringBuf(reinterpret_cast<const char*>(encoded), encodedSize));
        TScalar next{.Slot = value.Slot, .Bytes = std::move(bytes)};
        return std::optional<TScalar>(std::move(next));
    }

    return std::optional<TScalar>{};
}

TResult<TKeyType> TInterpreter::KeyType(const TExprNode& node, size_t depth) {
    Budget.Visit(node, depth);
    const TExprNode* type = &node;
    const bool nullable = type->IsCallable("OptionalType") && type->ChildrenSize() == 1;
    if (nullable) {
        type = &type->Head();
    }
    if (!type->IsCallable("DataType") || type->ChildrenSize() != 1 || !type->Head().IsAtom()) {
        return TError{EError::Unsupported};
    }

    Budget.Scan(type->Head().Content().size());
    const auto slot = NUdf::FindDataSlot(type->Head().Content());
    if (!slot || !Supported(*slot)) {
        return TError{EError::Unsupported};
    }

    return TKeyType{*slot, nullable};
}

TResult<TKeyType> TInterpreter::BoundaryKeyType(const TExprNode& node, size_t depth) {
    Budget.Visit(node, depth);
    if (!node.IsCallable("OptionalType") || node.ChildrenSize() != 1) {
        return TError{EError::Unsupported};
    }

    // Remove the outer optional that represents infinity. KeyType accepts a
    // scalar key or one nullable layer, and refuses deeper optional nesting.
    return KeyType(node.Head(), depth + 1);
}

TResult<int> TInterpreter::Compare(const TScalar& a, const TScalar& b) {
    // MiniKQL optional keys sort NULL before any present value. Present values
    // must have the same slot before either payload is read as that type.
    if (a.Null || b.Null) {
        return int(b.Null) - int(a.Null);
    }

    if (a.Slot != b.Slot) {
        return TError{EError::InvalidInput};
    }

    if (const auto x = Integer(a)) {
        const auto y = *Integer(b);
        return (*x > y) - (*x < y);
    }

    if (a.Slot == EDataSlot::Bool) {
        return int(a.Data.Get<bool>()) - int(b.Data.Get<bool>());
    }

    if (a.Slot != EDataSlot::String && a.Slot != EDataSlot::Utf8 && a.Slot != EDataSlot::Uuid) {
        return TError{EError::Unsupported};
    }

    // Bound the whole comparison, including the worst case of equal strings.
    const auto x = a.Text();
    const auto y = b.Text();
    const size_t length = Min(x.size(), y.size());
    Budget.Scan(length);
    if (length) {
        if (const int cmp = std::char_traits<char>::compare(x.data(), y.data(), length)) {
            return cmp;
        }
    }
    return int(x.size() > y.size()) - int(x.size() < y.size());
}

TResult<int> TInterpreter::Compare(const TBoundary& a, const TBoundary& b, bool inclusion) {
    Budget.Step();
    if (a.Components.size() != b.Components.size()) {
        return TError{EError::InvalidInput};
    }

    for (size_t i = 0; i < a.Components.size(); ++i) {
        const auto& x = a.Components[i];
        const auto& y = b.Components[i];
        if (x.Inf != y.Inf) {
            return x.Inf < y.Inf ? -1 : 1;
        }

        if (x.Inf) {
            break;
        }
        auto cmpResult = Compare(x.Value, y.Value);
        if (const auto* error = cmpResult.GetError()) {
            return *error;
        }

        const int cmp = *cmpResult.GetValue();
        if (cmp) {
            return cmp;
        }
    }
    // At equal key values: inclusive left < exclusive < inclusive right.
    // Callers comparing only key values explicitly disable this tie-breaker.
    return inclusion ? int(a.Included > b.Included) - int(a.Included < b.Included) : 0;
}

TResult<int> TInterpreter::Compare(const TRange& a, const TRange& b) {
    auto left = Compare(a.Left, b.Left);
    if (left.GetError() || *left.GetValue() != 0) {
        return left;
    }

    return Compare(a.Right, b.Right);
}

TResult<bool> TInterpreter::IsEmpty(const TRange& range) {
    auto cmpResult = Compare(range.Left, range.Right);
    if (const auto* error = cmpResult.GetError()) {
        return *error;
    }

    const int cmp = *cmpResult.GetValue();
    if (cmp >= 0) {
        return true;
    }

    if (range.Left.Included && range.Right.Included) {
        return false;
    }

    auto valuesResult = Compare(range.Left, range.Right, false);
    if (const auto* error = valuesResult.GetError()) {
        return *error;
    }

    const int values = *valuesResult.GetValue();
    return !values;
}

TRanges TInterpreter::NewRanges(size_t width) {
    Budget.Width(width);
    return {.Items = Budget.Vector<TRange>(), .Width = width};
}

TBoundary TInterpreter::NewBoundary(size_t width, i32 included) {
    Budget.Width(width);
    TBoundary result(Budget.Allocator<TComponent>());
    result.Components.reserve(width);
    result.Included = included;
    return result;
}

TBoundary TInterpreter::SingleBoundary(TComponent component, i32 included) {
    auto result = NewBoundary(1, included);
    result.Components.push_back(std::move(component));
    return result;
}

void TInterpreter::MoveAppend(TRanges& to, TRange&& range) {
    Budget.Ranges(to.Items.size() + 1);
    Budget.Step();

    // Growth and copies are charged by the containers, not by the operator.
    static_assert(std::is_nothrow_move_constructible_v<TRange>);
    to.Items.push_back(std::move(range));
}

void TInterpreter::Append(TRanges& to, const TRange& range) {
    MoveAppend(to, TRange(range));
}

TRanges TInterpreter::Copy(const TRanges& ranges) {
    auto result = NewRanges(ranges.Width);
    result.Finalized = ranges.Finalized;
    for (const auto& range : ranges.Items) {
        Append(result, range);
    }
    return result;
}

std::optional<TError> TInterpreter::Validate(const TRanges& ranges) {
    Budget.Width(ranges.Width);
    for (const auto& range : Budget.Each(ranges.Items)) {
        if (range.Left.Components.size() != ranges.Width || range.Right.Components.size() != ranges.Width) {
            return TError{EError::InvalidInput};
        }

        for (const auto* bound : {&range.Left, &range.Right}) {
            // A boundary is a finite key prefix followed by a consistent
            // infinite suffix. A finite component after infinity is malformed.
            bool infinity = false;
            for (const auto& c : bound->Components) {
                if (infinity && !c.Inf) {
                    return TError{EError::InvalidInput};
                }

                if (c.Inf) {
                    infinity = true;
                    if (c.Inf != InfSign(bound->HasPrefix(), bound->Included != 0, bound == &range.Left)) {
                        return TError{EError::InvalidInput};
                    }
                }
            }
        }
    }
    return std::nullopt;
}

// MiniKQL TRangeComputeBase: sort, remove empty ranges, remove duplicates. It
// does NOT merge overlapping ranges here; merging belongs to RangeUnion.
TResult<TRanges> TInterpreter::Normalize(TRanges ranges) {
    if (const auto error = Validate(ranges)) {
        return *error;
    }

    bool sorted = true;
    for (size_t i : Budget.Indices(1, ranges.Items.size())) {
        auto cmpResult = Compare(ranges.Items[i - 1], ranges.Items[i]);
        if (const auto* error = cmpResult.GetError()) {
            return *error;
        }

        const int cmp = *cmpResult.GetValue();
        if (cmp > 0) {
            sorted = false;
            break;
        }
    }

    if (!sorted) {
        const size_t count = ranges.Items.size();
        auto scratch = Budget.Vector<TRange>();
        scratch.resize(count);

        // Fallible merge sort: comparisons may decline unsupported values.
        // Every pass visits bounded items, with no sort-cost estimate.
        for (size_t width = 1; width < count; width *= 2) {
            for (size_t start = 0; start < count; start += 2 * width) {
                size_t a = start;
                size_t b = Min(start + width, count);
                const size_t middle = b;
                const size_t end = Min(start + 2 * width, count);
                for (size_t out : Budget.Indices(start, end)) {
                    bool takeA = b == end;
                    if (a < middle && b < end) {
                        auto cmpResult = Compare(ranges.Items[a], ranges.Items[b]);
                        if (const auto* error = cmpResult.GetError()) {
                            return *error;
                        }

                        const int cmp = *cmpResult.GetValue();
                        takeA = cmp <= 0;
                    }
                    const size_t index = takeA ? a++ : b++;
                    scratch[out] = std::move(ranges.Items[index]);
                }
            }
            ranges.Items.swap(scratch);
        }
    }

    size_t out = 0;
    for (size_t i : Budget.Indices(ranges.Items.size())) {
        auto emptyResult = IsEmpty(ranges.Items[i]);
        if (const auto* error = emptyResult.GetError()) {
            return *error;
        }

        const bool empty = *emptyResult.GetValue();
        if (empty) {
            continue;
        }
        if (out) {
            auto cmpResult = Compare(ranges.Items[out - 1], ranges.Items[i]);
            if (const auto* error = cmpResult.GetError()) {
                return *error;
            }

            const int cmp = *cmpResult.GetValue();
            if (!cmp) {
                continue;
            }
        }
        if (i != out) {
            ranges.Items[out] = std::move(ranges.Items[i]);
        }
        ++out;
    }
    ranges.Items.resize(out);
    return ranges;
}

TResult<TRanges> TInterpreter::Merge(TRanges ranges) {
    auto sortedResult = Normalize(std::move(ranges));
    if (const auto* error = sortedResult.GetError()) {
        return *error;
    }

    auto sorted = std::move(*sortedResult.GetValue());
    size_t out = 0;
    for (size_t i : Budget.Indices(sorted.Items.size())) {
        if (out) {
            auto cmpResult = Compare(sorted.Items[i].Left, sorted.Items[out - 1].Right);
            if (const auto* error = cmpResult.GetError()) {
                return *error;
            }

            const int cmp = *cmpResult.GetValue();

            // Equal, exclusive endpoints leave a hole; any included endpoint
            // sorts strictly across the opposite boundary's inclusion marker.
            if (cmp < 0) {
                auto rightResult = Compare(sorted.Items[i].Right, sorted.Items[out - 1].Right);
                if (const auto* error = rightResult.GetError()) {
                    return *error;
                }

                const int right = *rightResult.GetValue();
                if (right > 0) {
                    sorted.Items[out - 1].Right = std::move(sorted.Items[i].Right);
                }
                continue;
            }
        }
        if (i != out) {
            sorted.Items[out] = std::move(sorted.Items[i]);
        }
        ++out;
    }
    sorted.Items.resize(out);
    return sorted;
}

// Decode AsRange's external boundary: key optionals followed by an Int32
// inclusion flag. An absent outer optional means infinity; an absent inner
// optional is a finite NULL key. The internal value keeps those cases distinct.
TResult<TBoundary> TInterpreter::Boundary(const TExprNode& node, bool left, size_t depth) {
    Budget.Visit(node, depth);
    if (!node.IsList() || node.ChildrenSize() < 2) {
        return TError{EError::InvalidInput};
    }

    const size_t width = node.ChildrenSize() - 1;
    Budget.Width(width);
    auto includedResult = Literal(node.Tail(), depth + 1);
    if (const auto* error = includedResult.GetError()) {
        return *error;
    }

    auto included = std::move(*includedResult.GetValue());
    if (included.Null || included.Slot != EDataSlot::Int32) {
        return TError{EError::InvalidInput};
    }

    const i32 flag = included.Data.Get<i32>();
    if (flag != 0 && flag != 1) {
        return TError{EError::InvalidInput};
    }

    auto result = NewBoundary(width, flag ? (left ? -1 : 1) : 0);
    bool infinity = false;
    for (size_t i = 0; i < width; ++i) {
        const auto& item = *node.Child(i);
        bool infinite = false;
        TScalar scalar;
        if (item.IsCallable("Nothing") && item.ChildrenSize() == 1) {
            if (!item.Head().IsCallable("OptionalType")) {
                return TError{EError::Unsupported};
            }

            infinite = true;
        } else if (item.IsCallable({"SafeCast", "StrictCast"}) && item.ChildrenSize() == 2 &&
                   item.Head().IsCallable("Nothing")) {
            if (item.Head().ChildrenSize() != 1) {
                return TError{EError::InvalidInput};
            }

            auto sourceResult = BoundaryKeyType(item.Head().Head(), depth + 2);
            if (const auto* error = sourceResult.GetError()) {
                return *error;
            }

            auto targetResult = BoundaryKeyType(item.Tail(), depth + 1);
            if (const auto* error = targetResult.GetError()) {
                return *error;
            }

            const auto source = *sourceResult.GetValue();
            const auto target = *targetResult.GetValue();
            if (source.Slot != target.Slot) {
                return TError{EError::Unsupported};
            }

            // With the same scalar type, increasing optional depth from one to
            // two wraps the absent value in Just: a finite NULL. Equal or reduced
            // depth preserves outer absence, hence infinity (including identity casts).
            infinite = source.Nullable || !target.Nullable;
            if (!infinite) {
                scalar = TScalar{.Null = true, .Slot = target.Slot};
            }
        } else {
            if (!item.IsCallable("Just") || item.ChildrenSize() != 1) {
                return TError{EError::Unsupported};
            }

            auto valueResult = Literal(item.Head(), depth + 1);
            if (const auto* error = valueResult.GetError()) {
                return *error;
            }

            scalar = std::move(*valueResult.GetValue());
        }
        if (infinity && !infinite) {
            return TError{EError::InvalidInput};
        }

        infinity |= infinite;
        const bool prefix = i ? result.HasPrefix() : !infinite;
        result.Components.push_back({.Inf = infinite ? InfSign(prefix, flag, left) : 0, .Value = std::move(scalar)});
    }
    return result;
}

// AsRange is a literal list constructor: preserve order, duplicates and empty
// intervals. MiniKQL normalizes operands when a range algebra operator uses them.
TResult<TRanges> TInterpreter::AsRange(const TExprNode& node, size_t depth) {
    Budget.Ranges(node.ChildrenSize());
    auto result = NewRanges();
    for (const auto& item : Budget.Each(node.Children())) {
        if (!item->IsList() || item->ChildrenSize() != 2) {
            return TError{EError::InvalidInput};
        }

        auto leftResult = Boundary(item->Head(), true, depth + 1);
        if (const auto* error = leftResult.GetError()) {
            return *error;
        }

        auto left = std::move(*leftResult.GetValue());

        auto rightResult = Boundary(item->Tail(), false, depth + 1);
        if (const auto* error = rightResult.GetError()) {
            return *error;
        }

        auto right = std::move(*rightResult.GetValue());
        if (left.Components.size() != right.Components.size()) {
            return TError{EError::InvalidInput};
        }

        if (!result.Items.empty() && result.Width != left.Components.size()) {
            return TError{EError::InvalidInput};
        }

        result.Width = left.Components.size();
        MoveAppend(result, {std::move(left), std::move(right)});
    }
    if (result.Items.empty()) {
        // Empty AsRange has no inferable key type.
        return TError{EError::Unsupported};
    }

    return result;
}

TResult<TRanges> TInterpreter::Empty(const TExprNode& node, size_t depth) {
    if (node.ChildrenSize() != 1) {
        return TError{EError::InvalidInput};
    }

    const auto& type = node.Head();
    const size_t width = type.IsCallable("TupleType") ? type.ChildrenSize() : 1;
    if (!width) {
        return TError{EError::InvalidInput};
    }

    Budget.Width(width);
    for (size_t i = 0; i < width; ++i) {
        const auto key = KeyType(type.IsCallable("TupleType") ? *type.Child(i) : type, depth + 1);
        if (const auto* error = key.GetError()) {
            return *error;
        }
    }
    return NewRanges(width);
}

TResult<TRanges> TInterpreter::RangeFor(const TExprNode& node, size_t depth) {
    if (node.ChildrenSize() != 3 || !node.Head().IsAtom()) {
        return TError{EError::InvalidInput};
    }

    auto typeResult = KeyType(node.Tail(), depth + 1);
    if (const auto* error = typeResult.GetError()) {
        return *error;
    }

    const auto type = *typeResult.GetValue();
    const auto op = node.Head().Content();
    if (op == "Exists" || op == "NotExists") {
        return MakeRangeFor(op, {}, type);
    }

    auto valueResult = Literal(*node.Child(1), depth + 1);
    if (const auto* error = valueResult.GetError()) {
        return *error;
    }

    return MakeRangeFor(op, std::move(*valueResult.GetValue()), type);
}

// Semantics of ExpandRangeFor in yql_opt_range.cpp. The supported conversions
// are integral widening/narrowing and String/Utf8; other casts decline explicitly.
TResult<TRanges> TInterpreter::MakeRangeFor(TStringBuf op, TScalar value, TKeyType type) {
    Budget.Step();

    // Predicates requiring a present key use NULL as an excluded lower bound
    // on nullable columns. Non-nullable columns start at negative infinity.
    const auto low =
        SingleBoundary(type.Nullable ? TComponent{.Value = {.Null = true, .Slot = type.Slot}} : TComponent{.Inf = -1});
    const auto high = SingleBoundary({.Inf = 1});
    auto result = NewRanges(1);
    const auto add = [&](const TBoundary& left, const TBoundary& right) {
        Append(result, {left, right});
    };

    if (op == "Exists" || op == "NotExists") {
        if (!type.Nullable) {
            return TError{EError::Unsupported};
        }

        if (op == "Exists") {
            add(low, high);
        } else {
            add(SingleBoundary({.Value = {.Null = true, .Slot = type.Slot}}, -1),
                SingleBoundary({.Value = {.Null = true, .Slot = type.Slot}}, 1));
        }
        return result;
    }

    if (op != "==" && op != "===" && op != "!=" && op != "<" && op != "<=" && op != ">" && op != ">=" &&
        op != "StartsWith" && op != "NotStartsWith") {
        return TError{EError::Unsupported};
    }

    if (value.Null) {
        if (op == "!=" || op == "NotStartsWith") {
            add(low, high);
        }
        return result;
    }

    if (IsInteger(type.Slot) && IsInteger(value.Slot)) {
        const auto integer = *Integer(value);
        const auto [min, max] = *IntegerLimits(type.Slot);
        if (integer < min || integer > max) {
            if (op == "!=") {
                add(low, high);
                return result;
            }

            if ((integer > max && (op == "<" || op == "<=")) || (integer < min && (op == ">" || op == ">="))) {
                value = IntegerValue(type.Slot, integer > max ? max : min);
                op = integer > max ? "<=" : ">=";
            } else {
                if (op != "==" && op != "===" && op != "<" && op != "<=" && op != ">" && op != ">=") {
                    return TError{EError::Unsupported};
                }

                return result;
            }
        } else {
            value = IntegerValue(type.Slot, integer);
        }
    } else if ((type.Slot == EDataSlot::String || type.Slot == EDataSlot::Utf8) &&
               (value.Slot == EDataSlot::String || value.Slot == EDataSlot::Utf8)) {
        if (type.Slot == EDataSlot::Utf8) {
            Budget.Scan(value.Text().size());
            if (!NYql::IsUtf8(std::string_view(value.Text().data(), value.Text().size()))) {
                return TError{EError::Unsupported};
            }
        }
        value.Slot = type.Slot;
    } else if (value.Slot != type.Slot) {
        return TError{EError::Unsupported};
    }

    const auto open = SingleBoundary({.Value = value}, 0);
    const auto left = SingleBoundary({.Value = value}, -1);
    const auto right = SingleBoundary({.Value = value}, 1);

    if (op == "StartsWith" || op == "NotStartsWith") {
        if (type.Slot != EDataSlot::String && type.Slot != EDataSlot::Utf8) {
            return TError{EError::Unsupported};
        }

        auto nextResult = NextPrefix(value);
        if (const auto* error = nextResult.GetError()) {
            return *error;
        }

        auto next = std::move(*nextResult.GetValue());
        TBoundary upper = high;
        if (next) {
            upper = SingleBoundary({.Value = std::move(*next)});
        }
        if (op == "StartsWith") {
            add(left, upper);
        } else {
            add(low, open);
            if (next) {
                upper.Included = -1;
                add(upper, high);
            }
        }
        // Runtime StartsWith is expressed through RangeIntersect/RangeUnion.
        return Normalize(std::move(result));
    }

    if (op == "===") {
        // The extractor's discrete equality form uses [value, value + 1) when
        // the successor exists. At the type maximum it remains a closed point.
        if (const auto limits = IntegerLimits(type.Slot); limits && *Integer(value) < limits->second) {
            add(left, SingleBoundary({.Value = IntegerValue(type.Slot, *Integer(value) + 1)}));
            return result;
        }

        op = "==";
    }

    if (op == "==") {
        add(left, right);
    } else if (op == "!=") {
        add(low, open);
        add(open, high);
    } else if (op == "<") {
        add(low, open);
    } else if (op == "<=") {
        add(low, right);
    } else if (op == ">") {
        add(open, high);
    } else {
        add(left, high);
    }
    return result;
}

TResult<TRanges> TInterpreter::Operand(const TExprNode::TPtr& node, size_t depth) {
    auto valueResult = Evaluate(node, depth);
    if (const auto* error = valueResult.GetError()) {
        return *error;
    }

    auto value = std::move(*valueResult.GetValue());
    const auto* ranges = value->GetRanges();
    if (!ranges || ranges->Finalized) {
        return TError{EError::Unsupported};
    }

    // Algebra mutates its working copy. Cached values retain the constructor's
    // original ordering and multiplicity for other consumers of the same node.
    return Normalize(Copy(*ranges));
}

TResult<TRanges> TInterpreter::Union(const TExprNode& node, size_t depth) {
    if (!node.ChildrenSize()) {
        return TError{EError::InvalidInput};
    }

    auto result = NewRanges();
    bool first = true;
    for (const auto& child : Budget.Each(node.Children())) {
        auto nextResult = Operand(child, depth + 1);
        if (const auto* error = nextResult.GetError()) {
            return *error;
        }

        auto next = std::move(*nextResult.GetValue());
        if (!first && result.Width != next.Width) {
            return TError{EError::InvalidInput};
        }

        first = false;
        result.Width = next.Width;
        for (auto& range : next.Items) {
            MoveAppend(result, std::move(range));
        }
    }
    return Merge(std::move(result));
}

TResult<TRanges> TInterpreter::Intersect(const TExprNode& node, size_t depth) {
    if (!node.ChildrenSize()) {
        return TError{EError::InvalidInput};
    }

    auto currentResult = Operand(node.HeadPtr(), depth + 1);
    if (const auto* error = currentResult.GetError()) {
        return *error;
    }

    auto current = std::move(*currentResult.GetValue());

    for (size_t i : Budget.Indices(1, node.ChildrenSize())) {
        auto nextResult = Operand(node.ChildPtr(i), depth + 1);
        if (const auto* error = nextResult.GetError()) {
            return *error;
        }

        auto next = std::move(*nextResult.GetValue());
        if (current.Width != next.Width) {
            return TError{EError::InvalidInput};
        }

        auto result = NewRanges(current.Width);
        size_t a = 0;
        size_t b = 0;
        while (a < current.Items.size() && b < next.Items.size()) {
            const auto& x = current.Items[a];
            const auto& y = next.Items[b];
            auto orderResult = Compare(x, y);
            if (const auto* error = orderResult.GetError()) {
                return *error;
            }

            const int order = *orderResult.GetValue();
            const bool minIsA = order < 0;

            auto rightResult = Compare(x.Right, y.Right);
            if (const auto* error = rightResult.GetError()) {
                return *error;
            }

            const int right = *rightResult.GetValue();
            TRange range{minIsA ? y.Left : x.Left, right < 0 ? x.Right : y.Right};
            auto emptyResult = IsEmpty(range);
            if (const auto* error = emptyResult.GetError()) {
                return *error;
            }

            const bool empty = *emptyResult.GetValue();
            if (!empty) {
                MoveAppend(result, std::move(range));
            }
            // Match MiniKQL's sweep even for overlapping operand ranges and
            // equal right endpoints; a generic set-intersection sweep can
            // otherwise change multiplicity before a later range operator.
            const bool advanceA = empty ? minIsA : (right ? right < 0 : !minIsA);
            if (advanceA) {
                ++a;
            } else {
                ++b;
            }
        }
        current = std::move(result);
    }
    return current;
}

TResult<ui64> TInterpreter::Limit(const TExprNode& node, size_t depth) {
    auto valueResult = Literal(node, depth);
    if (const auto* error = valueResult.GetError()) {
        return *error;
    }

    auto value = std::move(*valueResult.GetValue());
    if (value.Null || value.Slot != EDataSlot::Uint64) {
        return TError{EError::InvalidInput};
    }

    return value.Data.Get<ui64>();
}

// Only a point prefix can restrict later key columns, so it forms a Cartesian
// product with the suffix ranges. A non-point interval continues with infinities
// in the suffix columns; appending finite suffix keys would change its meaning.
TResult<TRanges> TInterpreter::MultiplyPair(const TRanges& first, const TRanges& second, ui64 limit) {
    auto result = NewRanges(first.Width + second.Width);
    for (const auto& a : Budget.Each(first.Items)) {
        if (second.Items.empty()) {
            break;
        }
        auto comparison = Compare(a.Left, a.Right, false);
        if (const auto* error = comparison.GetError()) {
            return *error;
        }

        const bool point = a.Left.Included && a.Right.Included && *comparison.GetValue() == 0;
        const size_t count = point ? second.Items.size() : 1;

        // MiniKQL may widen the range on its semantic limit. This prototype
        // declines that case; it never substitutes a truncated Cartesian product.
        if (result.Items.size() > limit || count > limit - result.Items.size()) {
            return TError{EError::Unsupported};
        }

        Budget.Ranges(result.Items.size() + count);
        for (size_t i : Budget.Indices(count)) {
            TRange range = a;
            range.Left.Components.reserve(result.Width);
            range.Right.Components.reserve(result.Width);
            for (auto pair :
                {std::pair{&range.Left, &second.Items[i].Left}, std::pair{&range.Right, &second.Items[i].Right}}) {
                auto& target = *pair.first;
                if (point) {
                    if (pair.second->Components.empty()) {
                        return TError{EError::InvalidInput};
                    }

                    if (!pair.second->Components.front().Inf) {
                        target.Included = pair.second->Included;
                    }
                    target.Components.insert(
                        target.Components.end(), pair.second->Components.begin(), pair.second->Components.end());
                } else {
                    const i32 inf = InfSign(target.HasPrefix(), target.Included != 0, pair.first == &range.Left);
                    for (size_t c = 0; c < second.Width; ++c) {
                        target.Components.push_back({.Inf = inf});
                    }
                }
            }
            MoveAppend(result, std::move(range));
        }
    }
    return result;
}

TResult<TRanges> TInterpreter::Multiply(const TExprNode& node, size_t depth) {
    if (node.ChildrenSize() < 2) {
        return TError{EError::InvalidInput};
    }

    auto limitResult = Limit(node.Head(), depth + 1);
    if (const auto* error = limitResult.GetError()) {
        return *error;
    }

    const ui64 limit = *limitResult.GetValue();

    auto currentResult = Operand(node.ChildPtr(1), depth + 1);
    if (const auto* error = currentResult.GetError()) {
        return *error;
    }

    auto current = std::move(*currentResult.GetValue());
    if (node.ChildrenSize() == 2) {
        if (current.Items.size() > limit) {
            return TError{EError::Unsupported};
        }

        return current;
    }

    for (size_t i : Budget.Indices(2, node.ChildrenSize())) {
        auto nextResult = Operand(node.ChildPtr(i), depth + 1);
        if (const auto* error = nextResult.GetError()) {
            return *error;
        }

        auto next = std::move(*nextResult.GetValue());

        auto productResult = MultiplyPair(current, next, limit);
        if (const auto* error = productResult.GetError()) {
            return *error;
        }

        current = std::move(*productResult.GetValue());
    }
    return current;
}

// Match RangeFinalize's discrete endpoint simplification: [k, k + 1) becomes
// [k, k], and (k, k + 1] becomes [k + 1, k + 1], including composite prefixes.
// Do not normalize again afterward: the runtime preserves the resulting list.
TResult<TRanges> TInterpreter::Finalize(const TExprNode& node, size_t depth) {
    if (node.ChildrenSize() != 1) {
        return TError{EError::InvalidInput};
    }

    auto operandResult = Operand(node.HeadPtr(), depth + 1);
    if (const auto* error = operandResult.GetError()) {
        return *error;
    }

    auto result = std::move(*operandResult.GetValue());

    for (auto& range : Budget.Each(result.Items)) {
        if (bool(range.Left.Included) == bool(range.Right.Included)) {
            continue;
        }
        size_t finite = 0;
        while (finite < result.Width && !range.Left.Components[finite].Inf && !range.Right.Components[finite].Inf) {
            ++finite;
        }
        if (!finite ||
            (finite < result.Width && (!range.Left.Components[finite].Inf || !range.Right.Components[finite].Inf))) {
            continue;
        }
        bool equal = true;
        for (size_t i = 0; i + 1 < finite; ++i) {
            auto cmpResult = Compare(range.Left.Components[i].Value, range.Right.Components[i].Value);
            if (const auto* error = cmpResult.GetError()) {
                return *error;
            }

            const int cmp = *cmpResult.GetValue();
            equal &= cmp == 0;
        }
        const auto& left = range.Left.Components[finite - 1].Value;
        const auto& right = range.Right.Components[finite - 1].Value;
        if (!equal || left.Null || right.Null || left.Slot != right.Slot || !IntegerLimits(left.Slot) ||
            *Integer(right) - *Integer(left) != 1) {
            continue;
        }
        if (range.Left.Included) {
            range.Right = range.Left;
        } else {
            range.Left = range.Right;
        }
        range.Left.Included = -1;
        range.Right.Included = 1;
        for (size_t i = finite; i < result.Width; ++i) {
            range.Left.Components[i].Inf = -1;
            range.Right.Components[i].Inf = 1;
        }
    }
    result.Finalized = true;
    return result;
}

bool UnaryLambda(const TExprNode& node) {
    return node.IsLambda() && node.ChildrenSize() == 2 && node.Head().Type() == TExprNode::Arguments &&
           node.Head().ChildrenSize() == 1 && node.Head().Head().Type() == TExprNode::Argument;
}

TResult<NDetail::TVector<TExprNode::TPtr>> TInterpreter::LiteralList(const TExprNode& node, size_t depth) {
    Budget.Visit(node, depth);
    if (node.IsCallable("Parameter")) {
        return TError{EError::NonConstant};
    }

    if (node.IsCallable("Just") && node.ChildrenSize() == 1) {
        return LiteralList(node.Head(), depth + 1);
    }

    if (node.IsCallable("AsList") || node.IsList()) {
        Budget.Ranges(node.ChildrenSize());
        return NDetail::TVector<TExprNode::TPtr>(
            node.Children().begin(), node.Children().end(), Budget.Allocator<TExprNode::TPtr>());
    }
    if (!node.IsCallable("Map") || node.ChildrenSize() != 2 || !UnaryLambda(node.Tail())) {
        return TError{EError::Unsupported};
    }

    auto tupleResult = LiteralList(node.Head(), depth + 1);
    if (const auto* error = tupleResult.GetError()) {
        return *error;
    }

    auto tuple = std::move(*tupleResult.GetValue());
    const auto& lambda = node.Tail();
    const auto& body = lambda.Tail();
    if (!body.IsCallable("AsList")) {
        return TError{EError::Unsupported};
    }

    Budget.Ranges(body.ChildrenSize());
    auto result = Budget.Vector<TExprNode::TPtr>();
    result.reserve(body.ChildrenSize());
    for (const auto& item : Budget.Each(body.Children())) {
        if (!item->IsCallable("Nth") || item->ChildrenSize() != 2 || item->HeadPtr() != lambda.Head().HeadPtr() ||
            !item->Tail().IsAtom()) {
            return TError{EError::Unsupported};
        }

        ui32 index;
        Budget.Scan(item->Tail().Content().size());
        if (!TryFromString(item->Tail().Content(), index) || index >= tuple.size()) {
            return TError{EError::InvalidInput};
        }

        result.push_back(tuple[index]);
    }
    return result;
}

// Recognize exactly the extractor's checked IN template. This routine is not a
// general interpreter for Map, FlatMap, lambdas or arbitrary IfPresent programs.
TResult<TRanges> TInterpreter::SqlIn(const TExprNode& node, size_t depth) {
    // Verify the whole IfPresent/IfStrict/Collect/Take template, including
    // shared node identities and matching limits, before interpreting its list.
    if (node.ChildrenSize() != 3 || !UnaryLambda(*node.Child(1)) || !node.Tail().IsCallable("RangeEmpty")) {
        return TError{EError::Unsupported};
    }

    const auto& lambda = *node.Child(1);
    const auto argument = lambda.Head().HeadPtr();
    const auto& body = lambda.Tail();
    if (!body.IsCallable("IfStrict") || body.ChildrenSize() != 3) {
        return TError{EError::Unsupported};
    }

    const auto& condition = body.Head();
    if (!condition.IsCallable(">") || condition.ChildrenSize() != 2 || !condition.Head().IsCallable("Length") ||
        condition.Head().ChildrenSize() != 1) {
        return TError{EError::Unsupported};
    }

    auto limitResult = Limit(condition.Tail(), depth + 1);
    if (const auto* error = limitResult.GetError()) {
        return *error;
    }

    const ui64 limit = *limitResult.GetValue();
    if (limit == Max<ui64>()) {
        return TError{EError::Unsupported};
    }

    const auto points = condition.Head().HeadPtr();
    if (!body.Tail().IsCallable("RangeUnion") || body.Tail().ChildrenSize() != 1 || body.Tail().HeadPtr() != points ||
        !points->IsCallable("Collect") || points->ChildrenSize() != 1) {
        return TError{EError::Unsupported};
    }

    const auto& take = points->Head();
    if (!take.IsCallable("Take") || take.ChildrenSize() != 2) {
        return TError{EError::Unsupported};
    }

    auto takeCountResult = Limit(take.Tail(), depth + 1);
    if (const auto* error = takeCountResult.GetError()) {
        return *error;
    }

    const ui64 takeCount = *takeCountResult.GetValue();
    if (takeCount != limit + 1) {
        return TError{EError::Unsupported};
    }

    const auto& flatMap = take.Head();
    if (!flatMap.IsCallable("FlatMap") || flatMap.ChildrenSize() != 2 || !UnaryLambda(flatMap.Tail())) {
        return TError{EError::Unsupported};
    }

    const auto& itemLambda = flatMap.Tail();
    const auto itemArg = itemLambda.Head().HeadPtr();
    const auto& multiply = itemLambda.Tail();
    if (!multiply.IsCallable("RangeMultiply") || multiply.ChildrenSize() < 2) {
        return TError{EError::Unsupported};
    }

    auto productLimitResult = Limit(multiply.Head(), depth + 1);
    if (const auto* error = productLimitResult.GetError()) {
        return *error;
    }

    const ui64 productLimit = *productLimitResult.GetValue();
    if (productLimit != limit) {
        return TError{EError::Unsupported};
    }

    const size_t width = multiply.ChildrenSize() - 1;
    Budget.Width(width);

    auto itemsResult = LiteralList(node.Head(), depth + 1);
    if (const auto* error = itemsResult.GetError()) {
        return *error;
    }

    auto items = std::move(*itemsResult.GetValue());

    // The runtime template falls back to a full range when the *produced* point
    // count exceeds the limit. Until that path is implemented, decline it.
    if (items.size() > limit) {
        return TError{EError::Unsupported};
    }

    if (!body.Child(1)->IsCallable("AsRange")) {
        return TError{EError::Unsupported};
    }

    auto fullResult = AsRange(*body.Child(1), depth + 1);
    if (const auto* error = fullResult.GetError()) {
        return *error;
    }

    auto full = std::move(*fullResult.GetValue());

    auto emptyResult = Empty(node.Tail(), depth + 1);
    if (const auto* error = emptyResult.GetError()) {
        return *error;
    }

    auto empty = std::move(*emptyResult.GetValue());
    if (full.Width != width || empty.Width != width || full.Items.size() != 1) {
        return TError{EError::Unsupported};
    }

    for (size_t i = 0; i < width; ++i) {
        if (full.Items.front().Left.Components[i].Inf != -1 || full.Items.front().Right.Components[i].Inf != 1) {
            return TError{EError::Unsupported};
        }
    }
    // Tuple IN may first apply an identity cast and reorder/project its fields.
    // Record the source field for each key position without executing lambdas.
    const TExprNode* tupleTypes = nullptr;
    auto keyOrder = Budget.Vector<ui32>();
    if (flatMap.HeadPtr() != argument) {
        const auto& order = flatMap.Head();
        if (!order.IsCallable("Map") || order.ChildrenSize() != 2 || !UnaryLambda(order.Tail()) ||
            !order.Tail().Tail().IsList()) {
            return TError{EError::Unsupported};
        }

        const auto& casts = order.Head();
        if (!casts.IsCallable("FlatMap") || casts.ChildrenSize() != 2 || casts.HeadPtr() != argument ||
            !UnaryLambda(casts.Tail())) {
            return TError{EError::Unsupported};
        }

        const auto& cast = casts.Tail().Tail();
        if (!cast.IsCallable("StrictCast") || cast.ChildrenSize() != 2 ||
            cast.HeadPtr() != casts.Tail().Head().HeadPtr() || !cast.Tail().IsCallable("OptionalType") ||
            cast.Tail().ChildrenSize() != 1 || !cast.Tail().Head().IsCallable("TupleType")) {
            return TError{EError::Unsupported};
        }

        tupleTypes = &cast.Tail().Head();
        Budget.Width(tupleTypes->ChildrenSize());
        Budget.Width(order.Tail().Tail().ChildrenSize());
        keyOrder.reserve(order.Tail().Tail().ChildrenSize());
        for (const auto& element : order.Tail().Tail().Children()) {
            if (!element->IsCallable("Nth") || element->ChildrenSize() != 2 ||
                element->HeadPtr() != order.Tail().Head().HeadPtr() || !element->Tail().IsAtom()) {
                return TError{EError::Unsupported};
            }

            Budget.Scan(element->Tail().Content().size());
            ui32 index;
            if (!TryFromString(element->Tail().Content(), index) || index >= tupleTypes->ChildrenSize()) {
                return TError{EError::InvalidInput};
            }

            keyOrder.push_back(index);
        }
    }

    // Decode each key column's equality predicate once, then apply it to rows.
    struct TColumn {
        std::optional<ui32> Element;
        TStringBuf Op;
        TKeyType Type;
    };

    auto columns = Budget.Vector<TColumn>();
    columns.reserve(width);
    for (size_t i = 1; i < multiply.ChildrenSize(); ++i) {
        const auto& range = *multiply.Child(i);
        if (!range.IsCallable("RangeFor") || range.ChildrenSize() != 3 || !range.Head().IsAtom({"==", "==="})) {
            return TError{EError::Unsupported};
        }

        std::optional<ui32> index;
        if (range.ChildPtr(1) != itemArg) {
            const auto& nth = *range.Child(1);
            if (!nth.IsCallable("Nth") || nth.ChildrenSize() != 2 || nth.HeadPtr() != itemArg || !nth.Tail().IsAtom()) {
                return TError{EError::Unsupported};
            }

            Budget.Scan(nth.Tail().Content().size());
            ui32 field;
            if (!TryFromString(nth.Tail().Content(), field) || field >= keyOrder.size()) {
                return TError{EError::Unsupported};
            }

            index = keyOrder[field];
        }
        auto typeResult = KeyType(range.Tail(), depth + 1);
        if (const auto* error = typeResult.GetError()) {
            return *error;
        }

        const auto type = *typeResult.GetValue();
        columns.push_back({index, range.Head().Content(), type});
    }

    auto result = NewRanges(width);
    for (const auto& item : Budget.Each(items)) {
        if (tupleTypes) {
            if (!item->IsList() || item->ChildrenSize() != tupleTypes->ChildrenSize()) {
                return TError{EError::Unsupported};
            }

            // Identity tuple casts only. A failed cast in an unused field can
            // discard the whole row, so validate every field, not just key fields.
            for (size_t i = 0; i < item->ChildrenSize(); ++i) {
                auto valueResult = Literal(*item->Child(i), depth + 1);
                if (const auto* error = valueResult.GetError()) {
                    return *error;
                }

                auto value = std::move(*valueResult.GetValue());

                auto typeResult = KeyType(*tupleTypes->Child(i), depth + 1);
                if (const auto* error = typeResult.GetError()) {
                    return *error;
                }

                const auto type = *typeResult.GetValue();
                if (type.Nullable || value.Null || value.Slot != type.Slot) {
                    return TError{EError::Unsupported};
                }
            }
        }
        auto row = NewRanges();
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& c = columns[i];
            if (c.Element && (!item->IsList() || *c.Element >= item->ChildrenSize())) {
                return TError{EError::InvalidInput};
            }

            auto valueResult = Literal(c.Element ? *item->Child(*c.Element) : *item, depth + 1);
            if (const auto* error = valueResult.GetError()) {
                return *error;
            }

            auto rangeResult = MakeRangeFor(c.Op, std::move(*valueResult.GetValue()), c.Type);
            if (const auto* error = rangeResult.GetError()) {
                return *error;
            }

            auto columnResult = Normalize(std::move(*rangeResult.GetValue()));
            if (const auto* error = columnResult.GetError()) {
                return *error;
            }

            auto column = std::move(*columnResult.GetValue());
            if (i) {
                auto productResult = MultiplyPair(row, column, limit);
                if (const auto* error = productResult.GetError()) {
                    return *error;
                }

                row = std::move(*productResult.GetValue());
            } else {
                row = std::move(column);
            }
        }
        if (result.Items.size() > limit || row.Items.size() > limit - result.Items.size()) {
            return TError{EError::Unsupported};
        }

        for (auto& range : row.Items) {
            MoveAppend(result, std::move(range));
        }
    }
    return Merge(std::move(result));
}

template <class T>
TResult<TValuePtr> TInterpreter::MakeValue(TResult<T> result) {
    if (const auto* error = result.GetError()) {
        return *error;
    }

    return TValuePtr(Budget.Make<TValue>(std::move(*result.GetValue())));
}

TResult<TValuePtr> TInterpreter::Conditional(const TExprNode& node, size_t depth) {
    if (node.ChildrenSize() != 3) {
        return TError{EError::InvalidInput};
    }

    auto conditionResult = Evaluate(node.HeadPtr(), depth + 1);
    if (const auto* error = conditionResult.GetError()) {
        return *error;
    }

    auto condition = std::move(*conditionResult.GetValue());
    const auto* scalar = condition->GetScalar();
    if (!scalar || scalar->Null || scalar->Slot != EDataSlot::Bool) {
        return TError{EError::Unsupported};
    }

    // The untaken branch neither consumes budget nor needs to be constant.
    return Evaluate(node.ChildPtr(scalar->Data.Get<bool>() ? 1 : 2), depth + 1);
}

TResult<TValuePtr> TInterpreter::Run(const TExprNode::TPtr& node, size_t depth) {
    // Each routine implements one callable. Keep dispatch separate from range
    // algorithms and from resource accounting in their primitive operations.
    if (node->IsCallable({"If", "IfStrict"})) {
        return Conditional(*node, depth);
    }

    if (node->IsCallable("AsRange")) {
        return MakeValue(AsRange(*node, depth));
    }

    if (node->IsCallable("RangeEmpty")) {
        return MakeValue(Empty(*node, depth));
    }

    if (node->IsCallable("RangeFor")) {
        return MakeValue(RangeFor(*node, depth));
    }

    if (node->IsCallable("RangeUnion")) {
        return MakeValue(Union(*node, depth));
    }

    if (node->IsCallable("RangeIntersect")) {
        return MakeValue(Intersect(*node, depth));
    }

    if (node->IsCallable("RangeMultiply")) {
        return MakeValue(Multiply(*node, depth));
    }

    if (node->IsCallable("RangeFinalize")) {
        return MakeValue(Finalize(*node, depth));
    }

    if (node->IsCallable("IfPresent")) {
        return MakeValue(SqlIn(*node, depth));
    }

    // Optional wrappers are accepted by range-bound decoding, but exposing an
    // unwrapped scalar as a generic Optional value would be semantically wrong.
    if (node->IsCallable({"Just", "Nothing"})) {
        return TError{EError::Unsupported};
    }

    // Lists, tuples, atoms and lambdas can be valid AST values outside this
    // prototype's value domains. Arguments retain their NonConstant diagnosis.
    if (!node->IsCallable() && node->Type() != TExprNode::Argument) {
        return TError{EError::Unsupported};
    }

    return MakeValue(Literal(*node, depth));
}

TResult<TValuePtr> TInterpreter::Evaluate(const TExprNode::TPtr& node, size_t depth) {
    if (!node) {
        return TError{EError::InvalidInput};
    }

    Budget.Visit(*node, depth);
    if (const auto it = Cache.find(node); it != Cache.end()) {
        return it->second;
    }

    auto valueResult = Run(node, depth);
    if (const auto* error = valueResult.GetError()) {
        return *error;
    }

    auto value = std::move(*valueResult.GetValue());
    Cache.emplace(node, value);
    return value;
}

}   // namespace

class TSession::TImpl {
public:
    explicit TImpl(TLimits limits)
        : Budget(limits)
        , Interpreter(Budget)
    {
    }

    TBudget Budget;
    TInterpreter Interpreter;
};

TSession::TSession(TLimits limits) noexcept
    : Limits_(limits)
{
}

TSession::~TSession() = default;

TUsage TSession::GetUsage() const {
    return Impl_ ? Impl_->Budget.Usage() : TUsage{};
}

TResult<TValuePtr> TSession::Evaluate(const NYql::TExprNode::TPtr& node) {
    // The only resource-exception boundary. Unwinding destroys unfinished
    // values; any previously cached successes and their charges remain valid.
    try {
        if (!Impl_) {
            Impl_ = std::make_unique<TImpl>(Limits_);
        }

        return Impl_->Interpreter.Evaluate(node);
    } catch (const NDetail::TBudgetExceeded& error) {
        return TError{EError::BudgetExceeded, error.Resource};
    } catch (const std::bad_alloc&) {
        return TError{EError::BudgetExceeded, EResource::Memory};
    }
}
}   // namespace NKikimr::NKqp::NEval
