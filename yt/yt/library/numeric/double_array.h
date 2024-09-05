#pragma once

#include <vector>
#include <algorithm>
#include <array>

#include <util/system/yassert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// "Double array" is a real valued vector in mathematical sense (not related to |std::vector|).
// This class is not intended for direct instantiation.
// If you just need a general purpose double valued vector, use |TDoubleArray| instead.
//
// This class can be used to create custom extensions of the double array abstraction.
// E.g. one might add custom factory methods and custom |operator()| overloads.
// In order to inherit all functionality of |TDoubleArray| (including all helper functions such as Min, Max, ForEach,
// Apply, and so on), one must derive from |TDoubleArrayBase<DimCnt, TDerived>|, where |DimCnt| is the desired number
// of dimensions and |TDerived| is the derived class
// (see https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern for more details on this technique).
// See |TDoubleArray| as an example.
template <size_t DimCnt, class TDerived>
class TDoubleArrayBase
{
public:
    using value_type = double;
    using iterator = double*;
    using const_iterator = const double*;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static constexpr size_t Size = DimCnt;

public:
    constexpr TDoubleArrayBase()
        : Values_({})
    { }

    constexpr TDoubleArrayBase(std::initializer_list<double> values)
    {
        Y_DEBUG_ABORT_UNLESS(values.size() == Size);
        std::copy(std::begin(values), std::end(values), std::begin(Values_));
    }

    Y_FORCE_INLINE int size() const
    {
        return Values_.size();
    }

    Y_FORCE_INLINE double& operator[](int i)
    {
        return Values_[i];
    }

    Y_FORCE_INLINE const double& operator[](int i) const
    {
        return Values_[i];
    }

    Y_FORCE_INLINE iterator begin()
    {
        return std::begin(Values_);
    }

    Y_FORCE_INLINE iterator end()
    {
        return std::end(Values_);
    }

    Y_FORCE_INLINE const_iterator begin() const
    {
        return std::begin(Values_);
    }

    Y_FORCE_INLINE const_iterator end() const
    {
        return std::end(Values_);
    }

    Y_FORCE_INLINE const_iterator cbegin() const
    {
        return std::cbegin(Values_);
    }

    Y_FORCE_INLINE const_iterator cend() const
    {
        return std::cend(Values_);
    }

    Y_FORCE_INLINE reverse_iterator rbegin()
    {
        return std::rbegin(Values_);
    }

    Y_FORCE_INLINE reverse_iterator rend()
    {
        return std::rend(Values_);
    }

    Y_FORCE_INLINE const_reverse_iterator rbegin() const
    {
        return std::rbegin(Values_);
    }

    Y_FORCE_INLINE const_reverse_iterator rend() const
    {
        return std::rend(Values_);
    }

    Y_FORCE_INLINE const_reverse_iterator crbegin() const
    {
        return std::crbegin(Values_);
    }

    Y_FORCE_INLINE const_reverse_iterator crend() const
    {
        return std::crend(Values_);
    }

    static constexpr TDerived FromDouble(double value)
    {
        TDerived result;
        std::fill(std::begin(result), std::end(result), value);
        return result;
    }

    static constexpr TDerived Zero()
    {
        return FromDouble(0.0);
    }

    static constexpr TDerived Ones()
    {
        return FromDouble(1.0);
    }

private:
    std::array<double, Size> Values_;

public:
    template <class TPredicate>
    static constexpr bool All(const TDerived& vec, TPredicate&& predicate)
    {
        return std::all_of(std::begin(vec), std::end(vec), std::forward<TPredicate>(predicate));
    }

    template <class TPredicate>
    static constexpr bool All(const TDerived& vec1, const TDerived& vec2, TPredicate&& predicate)
    {
        for (size_t i = 0; i < Size; i++) {
            if (!predicate(vec1[i], vec2[i])) {
                return false;
            }
        }
        return true;
    }

    template <class TPredicate>
    static constexpr bool All(const TDerived& vec1, const TDerived& vec2, const TDerived& vec3, TPredicate&& predicate)
    {
        for (size_t i = 0; i < Size; i++) {
            if (!predicate(vec1[i], vec2[i], vec3[i])) {
                return false;
            }
        }
        return true;
    }

    template <class TPredicate>
    static constexpr bool Any(const TDerived& vec, TPredicate&& predicate)
    {
        return std::any_of(std::begin(vec), std::end(vec), std::forward<TPredicate>(predicate));
    }

    template <class TPredicate>
    static constexpr bool Any(const TDerived& vec1, const TDerived& vec2, TPredicate&& predicate)
    {
        for (size_t i = 0; i < Size; i++) {
            if (predicate(vec1[i], vec2[i])) {
                return true;
            }
        }
        return false;
    }

    template <class TPredicate>
    static constexpr bool Any(const TDerived& vec1, const TDerived& vec2, const TDerived& vec3, TPredicate&& predicate)
    {
        for (size_t i = 0; i < Size; i++) {
            if (predicate(vec1[i], vec2[i], vec3[i])) {
                return true;
            }
        }
        return false;
    }

    template <class TOperation>
    static constexpr TDerived Apply(const TDerived& vec, TOperation&& op)
    {
        TDerived res = {};
        std::transform(std::begin(vec), std::end(vec), std::begin(res), std::forward<TOperation>(op));
        return res;
    }

    template <class TOperation>
    static constexpr TDerived Apply(const TDerived& vec1, const TDerived& vec2, TOperation&& op)
    {
        TDerived res = {};
        for (size_t i = 0; i < Size; i++) {
            res[i] = op(vec1[i], vec2[i]);
        }
        return res;
    }

    template <class TOperation>
    static constexpr TDerived Apply(const TDerived& vec1, const TDerived& vec2, const TDerived& vec3, TOperation&& op)
    {
        TDerived res = {};
        for (size_t i = 0; i < Size; i++) {
            res[i] = op(vec1[i], vec2[i], vec3[i]);
        }
        return res;
    }

    template <class TFunction>
    static constexpr void ForEach(const TDerived& vec, TFunction&& fn)
    {
        std::for_each(std::begin(vec), std::end(vec), std::forward<TFunction>(fn));
    }

    template <class TFunction>
    static constexpr void ForEach(const TDerived& vec1, const TDerived& vec2, TFunction&& fn)
    {
        for (size_t i = 0; i < Size; i++) {
            fn(vec1[i], vec2[i]);
        }
    }

    template <class TFunction>
    static constexpr void ForEach(const TDerived& vec1, const TDerived& vec2, const TDerived& vec3, TFunction&& fn)
    {
        for (size_t i = 0; i < Size; i++) {
            fn(vec1[i], vec2[i], vec3[i]);
        }
    }

    static constexpr bool Near(const TDerived& lhs, const TDerived& rhs, double precision)
    {
        return All(lhs, rhs, [&](auto x, auto y) { return std::abs(x - y) <= precision; });
    }

    static constexpr TDerived Max(const TDerived& lhs, const TDerived& rhs)
    {
        return TDerived::Apply(lhs, rhs, [](auto x, auto y) { return std::max(x, y); });
    }

    static constexpr TDerived Min(const TDerived& lhs, const TDerived& rhs)
    {
        return TDerived::Apply(lhs, rhs, [](auto x, auto y) { return std::min(x, y); });
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <size_t DimCnt, class TDerived>
std::true_type IsDoubleArrayImpl(const TDoubleArrayBase<DimCnt, TDerived>*);

std::false_type IsDoubleArrayImpl(const void*);

} // namespace NDetail

template <class T>
constexpr bool IsDoubleArray = decltype(NDetail::IsDoubleArrayImpl(std::declval<T*>()))::value;

////////////////////////////////////////////////////////////////////////////////

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr bool Dominates(const TDerived& lhs, const TDerived& rhs)
{
    return TDerived::All(lhs, rhs, [](auto x, auto y) { return x >= y; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr double MaxComponent(const TDerived& vec)
{
    double result = std::numeric_limits<double>::lowest();
    for (size_t i = 0; i < TDerived::Size; i++) {
        result = std::max(result, vec[i]);
    }
    return result;
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr double MinComponent(const TDerived& vec)
{
    double result = std::numeric_limits<double>::max();
    for (size_t i = 0; i < TDerived::Size; i++) {
        result = std::min(result, vec[i]);
    }
    return result;
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr bool operator==(const TDerived& lhs, const TDerived& rhs)
{
    return TDerived::All(lhs, rhs, [](auto x, auto y) { return x == y; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived operator+(const TDerived& lhs, const TDerived& rhs)
{
    return TDerived::Apply(lhs, rhs, [](auto x, auto y) { return x + y; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived operator-(const TDerived& lhs, const TDerived& rhs)
{
    return TDerived::Apply(lhs, rhs, [](auto x, auto y) { return x - y; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived operator-(const TDerived& lhs)
{
    return TDerived::Apply(lhs, [](auto x) { return -x; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr void operator+=(TDerived& lhs, const TDerived& rhs)
{
    for (size_t i = 0; i < TDerived::Size; i++) {
        lhs[i] += rhs[i];
    }
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr void operator-=(TDerived& lhs, const TDerived& rhs)
{
    for (size_t i = 0; i < TDerived::Size; i++) {
        lhs[i] -= rhs[i];
    }
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived operator*(const TDerived& vec, double scalar)
{
    return TDerived::Apply(vec, [=](auto x) { return x * scalar; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived operator/(const TDerived& vec, double scalar)
{
    return TDerived::Apply(vec, [=](auto x) { return x / scalar; });
}

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
constexpr TDerived Div(
    const TDerived& lhs,
    const TDerived& rhs,
    double zeroDivByZero,
    double oneDivByZero)
{
    return TDerived::Apply(lhs, rhs, [=](auto x, auto y) {
        if (y == 0) {
            if (x == 0) {
                return zeroDivByZero;
            } else {
                return oneDivByZero;
            }
        } else {
            return x / y;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

// "Double array" is a real valued vector in mathematical sense (not related to |std::vector|).
// This class can be used as a general purpose real valued vector. Please note that this class is final.
// If you want to make any extensions of the abstraction, use |TDoubleArrayBase| instead.
//
// Example of usage:
//     TDoubleArray<4> vec1 = {1, 2, 3, 4};
//     YT_VERIFY(vec1[3] == 4);
//     YT_VERIFY(TDoubleArray<4>::All(vec1, [] (double x) { return x > 0; }));
//     YT_VERIFY(MinComponent(vec1) == 1);
//
//     TDoubleArray<4> vec2 = {4, 3, 2, 1};
//     YT_VERIFY(vec1 + vec2 == TDoubleArray<4>::FromDouble(5));
//
//     // |vec1 * vec1| wouldn't work because multiplication is not defined for mathematical vectors.
//     auto vec1Square = TDoubleArray<4>::Apply(vec1, [] (double x) { return x * x; });
//     YT_VERIFY(TDoubleArray<4>::All(vec1, vec1Square, [] (double x, double y) { return y == x * x; }));
template <size_t DimCnt>
class TDoubleArray final : public TDoubleArrayBase<DimCnt, TDoubleArray<DimCnt>>
{
private:
    using TBase = TDoubleArrayBase<DimCnt, TDoubleArray>;

public:
    // For some reason, cannot use |TBase::TDoubleArrayBase| for constructor inheritance.
    using TDoubleArrayBase<DimCnt, TDoubleArray>::TDoubleArrayBase;

    using TBase::operator[];
};

static_assert(IsDoubleArray<TDoubleArray<3>>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
