#pragma once

#include <type_traits>

/**
 * A numerical equivalent of TMaybe designed to work specifically with numerical
 * types (int, long, char etc.), which uses a specific excluded value to mark
 * an unset value (instead of a bool value, used by TMaybe).
 *
 * @tparam T The type of the underlying value to store
 * @tparam EXCLUDED_VALUE The excluded value, which is used to designed "unset"
 */
template <class T, T EXCLUDED_VALUE>
class TNumericalMaybe {
private:
    static_assert(
        std::is_integral<T>::value,
        "Instantiation of TNumericalMaybe with a non-numeric type is ill-formed"
    );

    static_assert(
        !std::is_reference<T>::value,
        "Instantiation of TNumericalMaybe with a reference type is ill-formed"
    );

public:
    using value_type = T;
    using TValueType = T;

    TNumericalMaybe() noexcept : Value(EXCLUDED_VALUE) {
    }

    TNumericalMaybe(const TNumericalMaybe& other) noexcept = default;

    explicit TNumericalMaybe(T value) noexcept {
        Value = value;
    }

    TNumericalMaybe& operator = (TNumericalMaybe other) noexcept {
        Value = other.Value;
        return *this;
    }

    TNumericalMaybe& operator = (T other) noexcept {
        Value = other;
        return *this;
    }

    bool operator == (TNumericalMaybe other) const noexcept {
        return Value == other.Value;
    }

    bool operator != (TNumericalMaybe other) const noexcept {
        return !(*this == other);
    }

    bool operator == (T other) const {
        return TNumericalMaybe(other) == *this;
    }

    bool operator != (T other) const {
        return TNumericalMaybe(other) != *this;
    }

    /**
     * Reset the value of this object to the "unset" value.
     */
    Y_REINITIALIZES_OBJECT void Clear() noexcept {
        Value = EXCLUDED_VALUE;
    }

    /**
     * Check if the this object contains a real value (not "unset").
     *
     * @return True if the current value is a real value (not "unset")
     */
    bool Defined() const noexcept {
        return Value != EXCLUDED_VALUE;
    }

    /**
     * Retrieve the underlying value stored in this object.
     *
     * @warning Calling this function when the current value is "unset",
     *          will result in a runtime exception.
     *
     * @return The underlying stored in this object
     */
    T Get() const {
        Y_ABORT_UNLESS(Defined());
        return Value;
    }

private:
    T Value;
};

class IOutputStream;

template <class T, T EXCLUDED_VALUE>
IOutputStream& operator<<(IOutputStream& out Y_LIFETIME_BOUND, TNumericalMaybe<T, EXCLUDED_VALUE> maybe) {
    if (maybe.Defined()) {
        out << maybe.Get();
    } else {
        out << TStringBuf("(undefined)");
    }

    return out;
}
