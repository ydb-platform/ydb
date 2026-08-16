#pragma once

#include <util/generic/hash.h>
#include <util/stream/output.h>

#include <utility>

namespace NYql {

// A type-safe alternative to a typedef or a `using` alias.
//
// `using` aliases are transparent: two aliases of the same underlying type are
// interchangeable, so nothing stops values of different meaning (an id, an
// offset, a size) from being mixed up, compared, or passed to the wrong
// parameter:
//
//     using TUserId = ui64;
//     using TItemId = ui64;
//     TUserId userId = 1;
//     TItemId itemId = userId;      // compiles, but is almost certainly a bug
//     if (userId == itemId) { ... } // compiles, compares unrelated ids
//
// `TStrongAlias` fixes this by making every alias a distinct type, so prefer it
// over a typedef/`using` whenever the values represent different concepts even
// though they share the same underlying representation:
//
//     using TUserId = TStrongAlias<class TUserIdTag, ui64>;
//     using TItemId = TStrongAlias<class TItemIdTag, ui64>;
//     TUserId userId(1);
//     TItemId itemId = userId;       // does not compile
//     if (userId == itemId) { ... }  // does not compile
//     TUserId otherUserId = userId;  // compiles, same alias type
//
// `TTagType` is only ever used as a template parameter to distinguish
// instantiations from each other; an incomplete forward-declared type (as in
// the examples above) is enough, it never needs to be defined.
//
// `TStrongAlias` deliberately exposes only construction, comparison and access
// to the underlying value (via `Value()`, `operator*` and `operator->`) - not
// the full interface of `TUnderlyingType`. This is intentional: it prevents,
// for example, adding two ids together just because addition happens to be
// defined for the underlying integer type.
template <typename TTagType, typename TUnderlyingType>
class TStrongAlias {
public:
    using TUnderlying = TUnderlyingType;
    using TFuncParam = const std::remove_reference_t<TUnderlyingType>&;

    TStrongAlias() = default;

    constexpr explicit TStrongAlias(const TUnderlyingType& value)
        : Value_(value)
    {
    }

    constexpr explicit TStrongAlias(TUnderlyingType&& value) noexcept
        : Value_(std::move(value))
    {
    }

    constexpr TUnderlyingType* operator->() {
        return &Value_;
    }

    constexpr const TUnderlyingType* operator->() const {
        return &Value_;
    }

    constexpr TUnderlyingType& operator*() & {
        return Value_;
    }

    constexpr const TUnderlyingType& operator*() const& {
        return Value_;
    }

    constexpr TUnderlyingType&& operator*() && {
        return std::move(Value_);
    }

    constexpr const TUnderlyingType&& operator*() const&& {
        return std::move(Value_);
    }

    constexpr TUnderlyingType& Value() & {
        return Value_;
    }

    constexpr const TUnderlyingType& Value() const& {
        return Value_;
    }

    constexpr TUnderlyingType&& Value() && {
        return std::move(Value_);
    }

    constexpr const TUnderlyingType&& Value() const&& {
        return std::move(Value_);
    }

    constexpr explicit operator const TUnderlyingType&() const& {
        return Value_;
    }

    friend auto operator<=>(const TStrongAlias& lhs, const TStrongAlias& rhs) = default;
    friend bool operator==(const TStrongAlias& lhs, const TStrongAlias& rhs) = default;

private:
    TUnderlyingType Value_;
};

} // namespace NYql

template <typename TTagType, typename TUnderlyingType>
struct THash<NYql::TStrongAlias<TTagType, TUnderlyingType>> {
    size_t operator()(const NYql::TStrongAlias<TTagType, TUnderlyingType>& alias) const {
        return THash<TUnderlyingType>()(alias.Value());
    }
};

template <typename TTagType, typename TUnderlyingType>
struct TEqualTo<NYql::TStrongAlias<TTagType, TUnderlyingType>> {
    bool operator()(const NYql::TStrongAlias<TTagType, TUnderlyingType>& lhs,
                    const NYql::TStrongAlias<TTagType, TUnderlyingType>& rhs) const {
        return TEqualTo<TUnderlyingType>()(lhs.Value(), rhs.Value());
    }
};

template <typename TTagType, typename TUnderlyingType>
IOutputStream& operator<<(IOutputStream& out, const NYql::TStrongAlias<TTagType, TUnderlyingType>& value) {
    return out << value.Value();
}
