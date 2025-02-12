/*******************************************************************************
 * tlx/container/string_view.hpp
 *
 * A simplified string_view implementation for portability.
 *
 * Copyright (C) 2012-2015 Marshall Clow
 * Copyright (C) 2015 Beman Dawes
 *
 * Based on the StringRef implementation in LLVM (http://llvm.org) and
 * N3422 by Jeffrey Yasskin
 *   http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3442.html
 * Updated July 2015 to reflect the Library Fundamentals TS
 *   http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2015/n4480.html
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2020 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CONTAINER_STRING_VIEW_HEADER
#define TLX_CONTAINER_STRING_VIEW_HEADER

#include <algorithm>
#include <cstring>
#include <iterator>
#include <ostream>
#include <stdexcept>
#include <string>

#include <tlx/string/hash_djb2.hpp>

namespace tlx {

//! \addtogroup tlx_container
//! \{

/*!
 * StringView is a reference to a part of a string, consisting of only a char
 * pointer and a length. It does not have ownership of the substring and is used
 * mainly for temporary objects.
 */
class StringView
{
public:
    // types
    typedef char value_type;
    typedef char* pointer;
    typedef const char* const_pointer;
    typedef char& reference;
    typedef const char& const_reference;
    typedef const_pointer const_iterator;
    typedef const_iterator iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
    typedef const_reverse_iterator reverse_iterator;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static constexpr size_type npos = size_type(-1);

    //! default construction
    StringView() noexcept : ptr_(nullptr), size_(0) { }

    //! copy construction
    StringView(const StringView& rhs) noexcept = default;
    //! assignment
    StringView& operator = (const StringView& rhs) noexcept = default;

    //! assign a whole string
    StringView(const std::string& str) noexcept
        : ptr_(str.data()), size_(str.size()) { }

    //! constructing a StringView from a temporary string is a bad idea
    StringView(std::string&&) = delete;

    //! assign a whole C-style string
    StringView(const char* str) noexcept
        : ptr_(str), size_(str ? std::strlen(str) : 0) { }

    //! assign a C-style string with given length
    StringView(const char* str, size_type size) noexcept
        : ptr_(str), size_(size) { }

    //! assign a string iterator with given length
    StringView(const std::string::const_iterator& begin, size_t size) noexcept
        : ptr_(&(*begin)), size_(size) { }

    //! assign a string between two iterators
    StringView(const std::string::const_iterator& begin,
               const std::string::const_iterator& end) noexcept
        : StringView(begin, end - begin) { }

    //! \name iterators
    //! \{

    const_iterator begin() const noexcept { return ptr_; }
    const_iterator cbegin() const noexcept { return ptr_; }
    const_iterator end() const noexcept { return ptr_ + size_; }
    const_iterator cend() const noexcept { return ptr_ + size_; }

    const_reverse_iterator rbegin() const noexcept {
        return const_reverse_iterator(end());
    }
    const_reverse_iterator crbegin() const noexcept {
        return const_reverse_iterator(end());
    }
    const_reverse_iterator rend() const noexcept {
        return const_reverse_iterator(begin());
    }
    const_reverse_iterator crend() const noexcept {
        return const_reverse_iterator(begin());
    }

    //! \}

    //! \name capacity
    //! \{

    size_type size() const noexcept { return size_; }
    size_type length() const noexcept { return size_; }
    size_type max_size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }

    //! \}

    //! \name element access
    //! \{

    const_pointer data() const noexcept { return ptr_; }

    const_reference operator [] (size_type pos) const noexcept {
        return ptr_[pos];
    }

    const_reference at(size_t pos) const {
        if (pos >= size_)
            throw std::out_of_range("StringView::at");
        return ptr_[pos];
    }

    const_reference front() const { return ptr_[0]; }
    const_reference back() const { return ptr_[size_ - 1]; }

    //! \}

    //! \name modifiers
    //! \{

    void clear() noexcept { size_ = 0; }

    void remove_prefix(size_type n) {
        if (n > size_)
            n = size_;
        ptr_ += n;
        size_ -= n;
    }

    void remove_suffix(size_type n) {
        if (n > size_)
            n = size_;
        size_ -= n;
    }

    void swap(StringView& s) noexcept {
        std::swap(ptr_, s.ptr_);
        std::swap(size_, s.size_);
    }

    //! \}

    // StringView string operations
    explicit operator std::string() const {
        return std::string(begin(), end());
    }

    //! Returns the data of this StringView as a std::string
    std::string to_string() const { return std::string(ptr_, size_); }

    size_type copy(char* s, size_type n, size_type pos = 0) const {
        if (pos > size())
            throw std::out_of_range("StringView::copy");
        size_type rsize = std::min(n, size_ - pos);
        std::copy(data(), data() + rsize, s);
        return rsize;
    }

    StringView substr(size_type pos, size_type n = npos) const {
        if (pos > size())
            throw std::out_of_range("StringView::substr");
        return StringView(data() + pos, (std::min)(size() - pos, n));
    }

    //! \name comparisons
    //! \{

    int compare(StringView x) const noexcept {
        const int cmp = std::strncmp(ptr_, x.ptr_, std::min(size_, x.size_));
        return cmp != 0 ? cmp
               : (size_ == x.size_ ? 0 : size_ < x.size_ ? -1 : 1);
    }

    int compare(size_type pos1, size_type n1, StringView x) const noexcept {
        return substr(pos1, n1).compare(x);
    }

    int compare(size_type pos1, size_type n1, StringView x, size_type pos2,
                size_type n2) const {
        return substr(pos1, n1).compare(x.substr(pos2, n2));
    }

    int compare(const char* x) const {
        return compare(StringView(x));
    }

    int compare(size_type pos1, size_type n1, const char* x) const {
        return substr(pos1, n1).compare(StringView(x));
    }

    int compare(
        size_type pos1, size_type n1, const char* x, size_type n2) const {
        return substr(pos1, n1).compare(StringView(x, n2));
    }

    //! \}

    //! \name comparison operators
    //! \{

    //! Equality operator to compare a StringView with another StringView
    bool operator == (const StringView& other) const noexcept {
        return size_ == other.size_ &&
               std::equal(ptr_, ptr_ + size_, other.ptr_);
    }

    //! Inequality operator to compare a StringView with another StringView
    bool operator != (const StringView& other) const noexcept {
        return !(operator == (other));
    }

    //! Less operator to compare a StringView with another StringView
    //! lexicographically
    bool operator < (const StringView& other) const noexcept {
        return std::lexicographical_compare(
            ptr_, ptr_ + size_, other.ptr_, other.ptr_ + other.size_);
    }

    //! Greater than
    bool operator > (const StringView& other) const noexcept {
        return other.operator < (*this);
    }

    //! Less than or equal to
    bool operator <= (const StringView& other) const noexcept {
        return !other.operator < (*this);
    }

    //! Less than or equal to
    bool operator >= (const StringView& other) const noexcept {
        return !operator < (other);
    }

    //! \}

    //! \name searches
    //! \{

    bool starts_with(char c) const noexcept {
        return !empty() && c == front();
    }

    bool starts_with(StringView x) const noexcept {
        return size_ >= x.size_ && std::equal(ptr_, ptr_ + x.size_, x.ptr_);
    }

    bool ends_with(char c) const noexcept {
        return !empty() && c == back();
    }

    bool ends_with(StringView x) const noexcept {
        return size_ >= x.size_ &&
               std::equal(ptr_ + size_ - x.size_, ptr_ + size_, x.ptr_);
    }

    //! \}

    //! \name find
    //! \{

    size_type find(StringView s, size_type pos = 0) const noexcept {
        if (pos > size())
            return npos;
        if (s.empty())
            return pos;
        const_iterator iter =
            std::search(cbegin() + pos, cend(), s.cbegin(), s.cend());
        return iter == cend() ? npos : std::distance(cbegin(), iter);
    }
    size_type find(char c, size_type pos = 0) const noexcept {
        return find(StringView(&c, 1), pos);
    }
    size_type find(const char* s, size_type pos, size_type n) const noexcept {
        return find(StringView(s, n), pos);
    }
    size_type find(const char* s, size_type pos = 0) const noexcept {
        return find(StringView(s), pos);
    }

    //! \}

    //! \name rfind
    //! \{

    size_type rfind(StringView s, size_type pos = npos) const noexcept {
        if (size_ < s.size_)
            return npos;
        if (pos > size_ - s.size_)
            pos = size_ - s.size_;
        if (s.size_ == 0u) // an empty string is always found
            return pos;
        for (const char* cur = ptr_ + pos; ; --cur) {
            if (std::strncmp(cur, s.ptr_, s.size_) == 0)
                return cur - ptr_;
            if (cur == ptr_)
                return npos;
        }
    }
    size_type rfind(char c, size_type pos = npos) const noexcept {
        return rfind(StringView(&c, 1), pos);
    }
    size_type rfind(const char* s, size_type pos, size_type n) const noexcept {
        return rfind(StringView(s, n), pos);
    }
    size_type rfind(const char* s, size_type pos = npos) const noexcept {
        return rfind(StringView(s), pos);
    }

    //! \}

    //! \name find_first_of
    //! \{

    size_type find_first_of(StringView s, size_type pos = 0) const noexcept {
        if (pos >= size_ || s.size_ == 0)
            return npos;
        const_iterator iter =
            std::find_first_of(cbegin() + pos, cend(), s.cbegin(), s.cend());
        return iter == cend() ? npos : std::distance(cbegin(), iter);
    }
    size_type find_first_of(char c, size_type pos = 0) const noexcept {
        return find_first_of(StringView(&c, 1), pos);
    }
    size_type find_first_of(const char* s,
                            size_type pos, size_type n) const noexcept {
        return find_first_of(StringView(s, n), pos);
    }
    size_type find_first_of(const char* s, size_type pos = 0) const noexcept {
        return find_first_of(StringView(s), pos);
    }

    //! \}

    //! \name find_last_of
    //! \{

    size_type find_last_of(StringView s, size_type pos = npos) const noexcept {
        if (s.size_ == 0u)
            return npos;
        if (pos >= size_)
            pos = 0;
        else
            pos = size_ - (pos + 1);
        const_reverse_iterator iter =
            std::find_first_of(crbegin() + pos, crend(), s.cbegin(), s.cend());
        return iter == crend() ? npos : reverse_distance(crbegin(), iter);
    }
    size_type find_last_of(char c, size_type pos = npos) const noexcept {
        return find_last_of(StringView(&c, 1), pos);
    }
    size_type find_last_of(const char* s,
                           size_type pos, size_type n) const noexcept {
        return find_last_of(StringView(s, n), pos);
    }
    size_type find_last_of(const char* s, size_type pos = npos) const noexcept {
        return find_last_of(StringView(s), pos);
    }

    //! \}

    //! \name find_first_not_of
    //! \{

    size_type find_first_not_of(StringView s,
                                size_type pos = 0) const noexcept {
        if (pos >= size_)
            return npos;
        if (s.size_ == 0)
            return pos;
        const_iterator iter = find_not_of(cbegin() + pos, cend(), s);
        return iter == cend() ? npos : std::distance(cbegin(), iter);
    }
    size_type find_first_not_of(char c, size_type pos = 0) const noexcept {
        return find_first_not_of(StringView(&c, 1), pos);
    }
    size_type find_first_not_of(const char* s, size_type pos,
                                size_type n) const noexcept {
        return find_first_not_of(StringView(s, n), pos);
    }
    size_type find_first_not_of(const char* s,
                                size_type pos = 0) const noexcept {
        return find_first_not_of(StringView(s), pos);
    }

    //! \}

    //! \name find_last_not_of
    //! \{

    size_type find_last_not_of(StringView s,
                               size_type pos = npos) const noexcept {
        if (pos >= size_)
            pos = size_ - 1;
        if (s.size_ == 0u)
            return pos;
        pos = size_ - (pos + 1);
        const_reverse_iterator iter = find_not_of(crbegin() + pos, crend(), s);
        return iter == crend() ? npos : reverse_distance(crbegin(), iter);
    }
    size_type find_last_not_of(char c, size_type pos = npos) const noexcept {
        return find_last_not_of(StringView(&c, 1), pos);
    }
    size_type find_last_not_of(const char* s,
                               size_type pos, size_type n) const noexcept {
        return find_last_not_of(StringView(s, n), pos);
    }
    size_type find_last_not_of(const char* s,
                               size_type pos = npos) const noexcept {
        return find_last_not_of(StringView(s), pos);
    }

    //! \}

    // ostream
    friend std::ostream& operator << (std::ostream& os, const StringView& v) {
        os.write(v.data(), v.size());
        return os;
    }

private:
    template <typename r_iter>
    size_type reverse_distance(r_iter first, r_iter last) const noexcept {
        // Portability note here: std::distance is not NOEXCEPT, but calling it
        // with a StringView::reverse_iterator will not throw.
        return size_ - 1 - std::distance(first, last);
    }

    template <typename Iterator>
    Iterator find_not_of(Iterator first, Iterator last, StringView s) const
    noexcept {
        for ( ; first != last; ++first)
            if (0 == std::char_traits<char>::find(s.ptr_, s.size_, *first))
                return first;
        return last;
    }

    const char* ptr_;
    size_type size_;
};

/*----------------------------------------------------------------------------*/

//! make alias due to STL similarity
using string_view = StringView;

/*----------------------------------------------------------------------------*/
// comparison operators: StringView with std::string

//! equality operator to compare a StringView with a std::string
static inline
bool operator == (const StringView& a, const std::string& b) noexcept {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin());
}

//! equality operator to compare a StringView with a std::string
static inline
bool operator == (const std::string& a, const StringView& b) noexcept {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin());
}

//! inequality operator to compare a StringView with a std::string
static inline
bool operator != (const StringView& a, const std::string& b) noexcept {
    return !(a.operator == (b));
}

//! inequality operator to compare a StringView with a std::string
static inline
bool operator != (const std::string& a, const StringView& b) noexcept {
    return !(b.operator == (a));
}

//! less operator to compare a StringView with a std::string
//! lexicographically
static inline
bool operator < (const StringView& a, const std::string& b) noexcept {
    return std::lexicographical_compare(
        a.begin(), a.end(), b.begin(), b.end());
}

//! less operator to compare a StringView with a std::string
//! lexicographically
static inline
bool operator < (const std::string& a, const StringView& b) noexcept {
    return std::lexicographical_compare(
        a.begin(), a.end(), b.begin(), b.end());
}

static inline
bool operator > (const StringView& x, const std::string& y) noexcept {
    return x > StringView(y);
}

static inline
bool operator > (const std::string& x, const StringView& y) noexcept {
    return StringView(x) > y;
}

static inline
bool operator <= (const StringView& x, const std::string& y) noexcept {
    return x <= StringView(y);
}

static inline
bool operator <= (const std::string& x, const StringView& y) noexcept {
    return StringView(x) <= y;
}

static inline
bool operator >= (const StringView& x, const std::string& y) noexcept {
    return x >= StringView(y);
}

static inline
bool operator >= (const std::string& x, const StringView& y) noexcept {
    return StringView(x) >= y;
}

/*----------------------------------------------------------------------------*/
// comparison operators: StringView with const char*

//! equality operator to compare a StringView with a const char*
static inline
bool operator == (const StringView& x, const char* y) noexcept {
    return x == StringView(y);
}

//! equality operator to compare a StringView with a const char*
static inline
bool operator == (const char* x, const StringView& y) noexcept {
    return StringView(x) == y;
}

//! inequality operator to compare a StringView with a const char*
static inline
bool operator != (const StringView& x, const char* y) noexcept {
    return x != StringView(y);
}

//! inequality operator to compare a StringView with a const char*
static inline
bool operator != (const char* x, const StringView& y) noexcept {
    return StringView(x) != y;
}

static inline
bool operator < (const StringView& x, const char* y) noexcept {
    return x < StringView(y);
}

static inline
bool operator < (const char* x, const StringView& y) noexcept {
    return StringView(x) < y;
}

static inline
bool operator > (const StringView& x, const char* y) noexcept {
    return x > StringView(y);
}

static inline
bool operator > (const char* x, const StringView& y) noexcept {
    return StringView(x) > y;
}

static inline
bool operator <= (const StringView& x, const char* y) noexcept {
    return x <= StringView(y);
}

static inline
bool operator <= (const char* x, const StringView& y) noexcept {
    return StringView(x) <= y;
}

static inline
bool operator >= (const StringView& x, const char* y) noexcept {
    return x >= StringView(y);
}

static inline
bool operator >= (const char* x, const StringView& y) noexcept {
    return StringView(x) >= y;
}

//! \}

} // namespace tlx

namespace std {
template <>
struct hash<tlx::StringView> {
    size_t operator () (const tlx::StringView& sv) const {
        return tlx::hash_djb2(sv.data(), sv.size());
    }
};

} // namespace std

#endif // !TLX_CONTAINER_STRING_VIEW_HEADER

/******************************************************************************/
