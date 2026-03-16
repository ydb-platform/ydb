/* SPDX-License-Identifier: MIT */
/* Copyright (c) 2022 Max Bachmann */

#pragma once

#include <cassert>
#include <cstddef>
#include <iterator>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <stdint.h>
#include <sys/types.h>
#include <vector>

#include <rapidfuzz/details/type_traits.hpp>

namespace rapidfuzz {
namespace detail {

static inline void assume(bool b)
{
#if defined(__clang__)
    __builtin_assume(b);
#elif defined(__GNUC__) || defined(__GNUG__)
    if (!b) __builtin_unreachable();
#elif defined(_MSC_VER)
    __assume(b);
#endif
}

namespace to_begin_detail {
using std::begin;

template <typename CharT>
CharT* to_begin(CharT* s)
{
    return s;
}

template <typename T>
auto to_begin(T& x) -> decltype(begin(x))
{

    return begin(x);
}
} // namespace to_begin_detail

using to_begin_detail::to_begin;

namespace to_end_detail {
using std::end;

template <typename CharT>
CharT* to_end(CharT* s)
{
    assume(s != nullptr);
    while (*s != 0)
        ++s;

    return s;
}

template <typename T>
auto to_end(T& x) -> decltype(end(x))
{
    return end(x);
}
} // namespace to_end_detail

using to_end_detail::to_end;

template <typename Iter>
class Range {
    Iter _first;
    Iter _last;
    // todo we might not want to cache the size for iterators
    // that can can retrieve the size in O(1) time
    size_t _size;

public:
    using value_type = typename std::iterator_traits<Iter>::value_type;
    using iterator = Iter;
    using reverse_iterator = std::reverse_iterator<iterator>;

    Range(Iter first, Iter last) : _first(first), _last(last)
    {
        assert(std::distance(_first, _last) >= 0);
        _size = static_cast<size_t>(std::distance(_first, _last));
    }

    Range(Iter first, Iter last, size_t size) : _first(first), _last(last), _size(size)
    {}

    template <typename T>
    Range(T& x) : Range(to_begin(x), to_end(x))
    {}

    iterator begin() const noexcept
    {
        return _first;
    }
    iterator end() const noexcept
    {
        return _last;
    }

    reverse_iterator rbegin() const noexcept
    {
        return reverse_iterator(end());
    }
    reverse_iterator rend() const noexcept
    {
        return reverse_iterator(begin());
    }

    size_t size() const
    {
        return _size;
    }

    bool empty() const
    {
        return size() == 0;
    }
    explicit operator bool() const
    {
        return !empty();
    }

    template <typename... Dummy, typename IterCopy = Iter,
              typename = rapidfuzz::rf_enable_if_t<
                  std::is_base_of<std::random_access_iterator_tag,
                                  typename std::iterator_traits<IterCopy>::iterator_category>::value>>
    auto operator[](size_t n) const -> decltype(*_first)
    {
        return _first[static_cast<ptrdiff_t>(n)];
    }

    void remove_prefix(size_t n)
    {
        std::advance(_first, static_cast<ptrdiff_t>(n));
        _size -= n;
    }

    void remove_suffix(size_t n)
    {
        std::advance(_last, -static_cast<ptrdiff_t>(n));
        _size -= n;
    }

    Range subseq(size_t pos = 0, size_t count = std::numeric_limits<size_t>::max())
    {
        if (pos > size()) throw std::out_of_range("Index out of range in Range::substr");

        Range res = *this;
        res.remove_prefix(pos);
        if (count < res.size()) res.remove_suffix(res.size() - count);

        return res;
    }

    const value_type& front() const
    {
        return *_first;
    }

    const value_type& back() const
    {
        return *(_last - 1);
    }

    Range<reverse_iterator> reversed() const
    {
        return {rbegin(), rend(), _size};
    }

    friend std::ostream& operator<<(std::ostream& os, const Range& seq)
    {
        os << "[";
        for (auto x : seq)
            os << static_cast<uint64_t>(x) << ", ";
        os << "]";
        return os;
    }
};

template <typename Iter>
auto make_range(Iter first, Iter last) -> Range<Iter>
{
    return Range<Iter>(first, last);
}

template <typename T>
auto make_range(T& x) -> Range<decltype(to_begin(x))>
{
    return {to_begin(x), to_end(x)};
}

template <typename InputIt1, typename InputIt2>
inline bool operator==(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    if (a.size() != b.size()) return false;

    return std::equal(a.begin(), a.end(), b.begin());
}

template <typename InputIt1, typename InputIt2>
inline bool operator!=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(a == b);
}

template <typename InputIt1, typename InputIt2>
inline bool operator<(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()));
}

template <typename InputIt1, typename InputIt2>
inline bool operator>(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return b < a;
}

template <typename InputIt1, typename InputIt2>
inline bool operator<=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(b < a);
}

template <typename InputIt1, typename InputIt2>
inline bool operator>=(const Range<InputIt1>& a, const Range<InputIt2>& b)
{
    return !(a < b);
}

template <typename InputIt>
using RangeVec = std::vector<Range<InputIt>>;

} // namespace detail
} // namespace rapidfuzz
