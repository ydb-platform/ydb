#pragma once

#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Convenience sentinel traits for structural types (integers, enums, pointers).
/*!
 *  Usage: |TSentinelOptional<int, TValueSentinel<-1>>|
 */
template <auto V>
struct TValueSentinel
{
    static constexpr auto Sentinel = V;
};

////////////////////////////////////////////////////////////////////////////////

//! A compact alternative to |std::optional<T>| that uses a sentinel value
//! to represent the null state rather than an additional boolean field.
/*!
 *  The null state is represented by |TSentinel::Sentinel| of type |T|.
 *  The stored representation is exactly one |T|, so:
 *    sizeof(TSentinelOptional<T, TSentinel>) == sizeof(T)
 *    alignof(TSentinelOptional<T, TSentinel>) == alignof(T)
 *
 *  Whenever |T| is trivially copyable the class is also trivially copyable,
 *  which guarantees that |std::atomic<TSentinelOptional<T, TSentinel>>| is lock-free
 *  if and only if |std::atomic<T>| is lock-free.
 *
 *  The interface is a drop-in replacement for |std::optional<T>|.
 *
 *  |TSentinel| must provide a |static constexpr T Sentinel| member.
 *  For structural types, use |TValueSentinel<V>| as a convenience.
 */
template <class T, class TSentinel>
class TSentinelOptional
{
public:
    using value_type = T;

    //! Constructs a null optional (stores the sentinel value).
    constexpr TSentinelOptional() = default;

    //! Constructs a null optional.
    constexpr TSentinelOptional(std::nullopt_t) noexcept;

    //! Constructs an optional holding |value|.
    /*!
     *  \note The behavior is undefined if |value == TSentinel::Sentinel|.
     */
    constexpr TSentinelOptional(T value) noexcept;

    //! Converts from |std::optional<T>|; nullopt maps to the null state.
    constexpr TSentinelOptional(std::optional<T> opt) noexcept;

    constexpr TSentinelOptional(const TSentinelOptional&) = default;
    constexpr TSentinelOptional(TSentinelOptional&&) = default;

    constexpr TSentinelOptional& operator=(const TSentinelOptional&) = default;
    constexpr TSentinelOptional& operator=(TSentinelOptional&&) = default;

    //! Resets to null.
    constexpr TSentinelOptional& operator=(std::nullopt_t) noexcept;

    //! Assigns |value|.
    /*!
     *  \note The behavior is undefined if |value == TSentinel::Sentinel|.
     */
    constexpr TSentinelOptional& operator=(T value) noexcept;

    //! Returns |true| iff the optional holds a value (i.e., the stored value differs from the sentinel).
    [[nodiscard]] constexpr bool has_value() const noexcept;

    //! Returns |true| iff the optional holds a value.
    constexpr explicit operator bool() const noexcept;

    //! Returns a reference to the held value; behavior is undefined if null.
    constexpr T& operator*() noexcept;
    constexpr const T& operator*() const noexcept;

    //! Returns a pointer to the held value; behavior is undefined if null.
    constexpr T* operator->() noexcept;
    constexpr const T* operator->() const noexcept;

    //! Returns a reference to the held value; aborts if null.
    T& value() noexcept;
    const T& value() const noexcept;

    //! Returns the held value if present, otherwise |default_value|.
    constexpr T value_or(T default_value) const noexcept;

    //! Resets to null (equivalent to assigning |std::nullopt|).
    constexpr void reset() noexcept;

    //! Converts to |std::optional<T>|; the null state maps to nullopt.
    constexpr operator std::optional<T>() const noexcept;

    constexpr bool operator==(const TSentinelOptional& other) const = default;
    [[nodiscard]] constexpr bool operator==(std::nullopt_t) const noexcept;

private:
    T Value_ = TSentinel::Sentinel;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SENTINEL_OPTIONAL_INL_H_
#include "sentinel_optional-inl.h"
#undef SENTINEL_OPTIONAL_INL_H_
