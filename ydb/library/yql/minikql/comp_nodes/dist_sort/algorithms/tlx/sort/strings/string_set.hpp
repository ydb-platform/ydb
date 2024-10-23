/*******************************************************************************
 * tlx/sort/strings/string_set.hpp
 *
 * Implementations of a StringSet concept. This is an internal implementation
 * header, see tlx/sort/strings.hpp for public front-end functions.
 *
 * A StringSet abstracts from arrays of strings, we provide four abstractions:
 *
 * - UCharStringSet: (const) unsigned char**
 * - StdStringSet: std::string*
 * - UPtrStdStringSet: std::unique_ptr<std::string>*
 * - StringSuffixSet: suffix sorting indexes of a std::string text
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SORT_STRINGS_STRING_SET_HEADER
#define TLX_SORT_STRINGS_STRING_SET_HEADER

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../logger/core.hpp"
#include "../../math/bswap.hpp"
#include "../../meta/enable_if.hpp"

namespace tlx {

//! \addtogroup tlx_sort
//! \{

namespace sort_strings_detail {

/******************************************************************************/

/*!
 * Base class for common string set functions, included via CRTP.
 */
template <typename StringSet, typename Traits>
class StringSetBase
{
public:
    //! index-based array access (readable and writable) to String objects.
    typename Traits::String& at(size_t i) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return *(ss.begin() + i);
    }

    //! \name CharIterator Comparisons
    //! \{

    //! check equality of two strings a and b at char iterators ai and bi.
    bool is_equal(const typename Traits::String& a,
                  const typename Traits::CharIterator& ai,
                  const typename Traits::String& b,
                  const typename Traits::CharIterator& bi) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return !ss.is_end(a, ai) && !ss.is_end(b, bi) && (*ai == *bi);
    }

    //! check if string a is less or equal to string b at iterators ai and bi.
    bool is_less(const typename Traits::String& a,
                 const typename Traits::CharIterator& ai,
                 const typename Traits::String& b,
                 const typename Traits::CharIterator& bi) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return ss.is_end(a, ai) ||
               (!ss.is_end(a, ai) && !ss.is_end(b, bi) && *ai < *bi);
    }

    //! check if string a is less or equal to string b at iterators ai and bi.
    bool is_leq(const typename Traits::String& a,
                const typename Traits::CharIterator& ai,
                const typename Traits::String& b,
                const typename Traits::CharIterator& bi) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return ss.is_end(a, ai) ||
               (!ss.is_end(a, ai) && !ss.is_end(b, bi) && *ai <= *bi);
    }

    //! \}

    //! \name Character Extractors
    //! \{

    typename Traits::Char
    get_char(const typename Traits::String& s, size_t depth) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return *ss.get_chars(s, depth);
    }

    //! Return up to 1 characters of string s at iterator i packed into a
    //! uint8_t (only works correctly for 8-bit characters)
    std::uint8_t get_uint8(
        const typename Traits::String& s, typename Traits::CharIterator i) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        if (ss.is_end(s, i)) return 0;
        return std::uint8_t(*i);
    }

    //! Return up to 2 characters of string s at iterator i packed into a
    //! uint16_t (only works correctly for 8-bit characters)
    std::uint16_t get_uint16(
        const typename Traits::String& s, typename Traits::CharIterator i) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        std::uint16_t v = 0;
        if (ss.is_end(s, i)) return v;
        v = (std::uint16_t(*i) << 8);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint16_t(*i) << 0);
        return v;
    }

    //! Return up to 4 characters of string s at iterator i packed into a
    //! uint32_t (only works correctly for 8-bit characters)
    std::uint32_t get_uint32(
        const typename Traits::String& s, typename Traits::CharIterator i) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        std::uint32_t v = 0;
        if (ss.is_end(s, i)) return v;
        v = (std::uint32_t(*i) << 24);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint32_t(*i) << 16);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint32_t(*i) << 8);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint32_t(*i) << 0);
        return v;
    }

    //! Return up to 8 characters of string s at iterator i packed into a
    //! uint64_t (only works correctly for 8-bit characters)
    std::uint64_t get_uint64(
        const typename Traits::String& s, typename Traits::CharIterator i) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        std::uint64_t v = 0;
        if (ss.is_end(s, i)) return v;
        v = (std::uint64_t(*i) << 56);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 48);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 40);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 32);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 24);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 16);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 8);
        ++i;
        if (ss.is_end(s, i)) return v;
        v |= (std::uint64_t(*i) << 0);
        return v;
    }

    std::uint8_t get_uint8(const typename Traits::String& s, size_t depth) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return get_uint8(s, ss.get_chars(s, depth));
    }

    std::uint16_t get_uint16(const typename Traits::String& s, size_t depth) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return get_uint16(s, ss.get_chars(s, depth));
    }

    std::uint32_t get_uint32(const typename Traits::String& s, size_t depth) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return get_uint32(s, ss.get_chars(s, depth));
    }

    std::uint64_t get_uint64(const typename Traits::String& s, size_t depth) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return get_uint64(s, ss.get_chars(s, depth));
    }

    //! \}

    //! Subset this string set using index range.
    StringSet subi(size_t begin, size_t end) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        return ss.sub(ss.begin() + begin, ss.begin() + end);
    }

    bool check_order(const typename Traits::String& s1,
                     const typename Traits::String& s2) const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        typename StringSet::CharIterator c1 = ss.get_chars(s1, 0);
        typename StringSet::CharIterator c2 = ss.get_chars(s2, 0);

        while (ss.is_equal(s1, c1, s2, c2))
            ++c1, ++c2;

        return ss.is_leq(s1, c1, s2, c2);
    }

    bool check_order() const {
        const StringSet& ss = *static_cast<const StringSet*>(this);

        for (typename Traits::Iterator pi = ss.begin();
             pi + 1 != ss.end(); ++pi)
        {
            if (!check_order(*pi, *(pi + 1))) {
                TLX_LOG1 << "check_order() failed at position " << pi - ss.begin();
                return false;
            }
        }
        return true;
    }

    void print() const {
        const StringSet& ss = *static_cast<const StringSet*>(this);
        size_t i = 0;
        for (typename Traits::Iterator pi = ss.begin(); pi != ss.end(); ++pi)
        {
            TLX_LOG1 << "[" << i++ << "] = " << *pi
                     << " = " << ss.get_string(*pi, 0);
        }
    }
};

template <typename Type, typename StringSet>
inline
typename enable_if<sizeof(Type) == 4, std::uint32_t>::type
get_key(const StringSet& strset,
        const typename StringSet::String& s, size_t depth) {
    return strset.get_uint32(s, depth);
}

template <typename Type, typename StringSet>
inline
typename enable_if<sizeof(Type) == 8, std::uint64_t>::type
get_key(const StringSet& strset,
        const typename StringSet::String& s, size_t depth) {
    return strset.get_uint64(s, depth);
}

template <typename Type, typename StringSet>
inline
Type get_key_at(const StringSet& strset, size_t idx, size_t depth) {
    return get_key<Type>(strset, strset.at(idx), depth);
}

/******************************************************************************/

/*!
 * Traits class implementing StringSet concept for char* and unsigned char*
 * strings.
 */
template <typename CharType>
class GenericCharStringSetTraits
{
public:
    //! exported alias for character type
    typedef CharType Char;

    //! String reference: pointer to first character
    typedef Char* String;

    //! Iterator over string references: pointer over pointers
    typedef String* Iterator;

    //! iterator of characters in a string
    typedef const Char* CharIterator;

    //! exported alias for assumed string container
    typedef std::pair<Iterator, size_t> Container;
};

/*!
 * Class implementing StringSet concept for char* and unsigned char* strings.
 */
template <typename CharType>
class GenericCharStringSet
    : public GenericCharStringSetTraits<CharType>,
      public StringSetBase<GenericCharStringSet<CharType>,
                           GenericCharStringSetTraits<CharType> >
{
public:
    typedef GenericCharStringSetTraits<CharType> Traits;

    typedef typename Traits::Char Char;
    typedef typename Traits::String String;
    typedef typename Traits::Iterator Iterator;
    typedef typename Traits::CharIterator CharIterator;
    typedef typename Traits::Container Container;

    //! Construct from begin and end string pointers
    GenericCharStringSet(Iterator begin, Iterator end)
        : begin_(begin), end_(end)
    { }

    //! Construct from a string container
    explicit GenericCharStringSet(const Container& c)
        : begin_(c.first), end_(c.first + c.second)
    { }

    //! Return size of string array
    size_t size() const { return end_ - begin_; }
    //! Iterator representing first String position
    Iterator begin() const { return begin_; }
    //! Iterator representing beyond last String position
    Iterator end() const { return end_; }

    //! Iterator-based array access (readable and writable) to String objects.
    String& operator [] (Iterator i) const
    { return *i; }

    //! Return CharIterator for referenced string, which belong to this set.
    CharIterator get_chars(const String& s, size_t depth) const
    { return s + depth; }

    //! Returns true if CharIterator is at end of the given String
    bool is_end(const String&, const CharIterator& i) const
    { return (*i == 0); }

    //! Return complete string (for debugging purposes)
    std::string get_string(const String& s, size_t depth = 0) const
    { return std::string(reinterpret_cast<const char*>(s) + depth); }

    //! Subset this string set using iterator range.
    GenericCharStringSet sub(Iterator begin, Iterator end) const
    { return GenericCharStringSet(begin, end); }

    //! Allocate a new temporary string container with n empty Strings
    static Container allocate(size_t n)
    { return std::make_pair(new String[n], n); }

    //! Deallocate a temporary string container
    static void deallocate(Container& c)
    { delete[] c.first; c.first = nullptr; }

protected:
    //! array of string pointers
    Iterator begin_, end_;
};

typedef GenericCharStringSet<char> CharStringSet;
typedef GenericCharStringSet<unsigned char> UCharStringSet;

typedef GenericCharStringSet<const char> CCharStringSet;
typedef GenericCharStringSet<const unsigned char> CUCharStringSet;

/******************************************************************************/

/*!
 * Class implementing StringSet concept for a std::string objects.
 */
class StdStringSetTraits
{
public:
    //! exported alias for character type
    typedef std::uint8_t Char;

    //! String reference: std::string, which should be reference counted.
    typedef std::string String;

    //! Iterator over string references: pointer to std::string.
    typedef String* Iterator;

    //! iterator of characters in a string
    typedef const Char* CharIterator;

    //! exported alias for assumed string container
    typedef std::pair<Iterator, size_t> Container;
};

/*!
 * Class implementing StringSet concept for arrays of std::string objects.
 */
class StdStringSet
    : public StdStringSetTraits,
      public StringSetBase<StdStringSet, StdStringSetTraits>
{
public:
    //! Construct from begin and end string pointers
    StdStringSet(const Iterator& begin, const Iterator& end)
        : begin_(begin), end_(end)
    { }

    //! Construct from a string container
    explicit StdStringSet(Container& c)
        : begin_(c.first), end_(c.first + c.second)
    { }

    //! Return size of string array
    size_t size() const { return end_ - begin_; }
    //! Iterator representing first String position
    Iterator begin() const { return begin_; }
    //! Iterator representing beyond last String position
    Iterator end() const { return end_; }

    //! Array access (readable and writable) to String objects.
    String& operator [] (const Iterator& i) const
    { return *i; }

    //! Return CharIterator for referenced string, which belongs to this set.
    CharIterator get_chars(const String& s, size_t depth) const
    { return reinterpret_cast<CharIterator>(s.data()) + depth; }

    //! Returns true if CharIterator is at end of the given String
    bool is_end(const String& s, const CharIterator& i) const
    { return (i >= reinterpret_cast<CharIterator>(s.data()) + s.size()); }

    //! Return complete string (for debugging purposes)
    std::string get_string(const String& s, size_t depth = 0) const
    { return s.substr(depth); }

    //! Subset this string set using iterator range.
    StdStringSet sub(Iterator begin, Iterator end) const
    { return StdStringSet(begin, end); }

    //! Allocate a new temporary string container with n empty Strings
    static Container allocate(size_t n)
    { return std::make_pair(new String[n], n); }

    //! Deallocate a temporary string container
    static void deallocate(Container& c)
    { delete[] c.first; c.first = nullptr; }

protected:
    //! pointers to std::string objects
    Iterator begin_, end_;
};

/******************************************************************************/

/*!
 * Class implementing StringSet concept for a std::unique_ptr<std::string
 * objects, which are non-copyable.
 */
class UPtrStdStringSetTraits
{
public:
    //! exported alias for character type
    typedef std::uint8_t Char;

    //! String reference: std::string, which should be reference counted.
    typedef std::unique_ptr<std::string> String;

    //! Iterator over string references: using std::vector's iterator
    typedef String* Iterator;

    //! iterator of characters in a string
    typedef const Char* CharIterator;

    //! exported alias for assumed string container
    typedef std::pair<Iterator, size_t> Container;
};

/*!
 * Class implementing StringSet concept for a std::vector containing std::string
 * objects.
 */
class UPtrStdStringSet
    : public UPtrStdStringSetTraits,
      public StringSetBase<UPtrStdStringSet, UPtrStdStringSetTraits>
{
public:
    //! Construct from begin and end string pointers
    UPtrStdStringSet(const Iterator& begin, const Iterator& end)
        : begin_(begin), end_(end)
    { }

    //! Construct from a string container
    explicit UPtrStdStringSet(Container& c)
        : begin_(c.first), end_(c.first + c.second)
    { }

    //! Return size of string array
    size_t size() const { return end_ - begin_; }
    //! Iterator representing first String position
    Iterator begin() const { return begin_; }
    //! Iterator representing beyond last String position
    Iterator end() const { return end_; }

    //! Array access (readable and writable) to String objects.
    String& operator [] (const Iterator& i) const
    { return *i; }

    //! Return CharIterator for referenced string, which belongs to this set.
    CharIterator get_chars(const String& s, size_t depth) const
    { return reinterpret_cast<CharIterator>(s->data()) + depth; }

    //! Returns true if CharIterator is at end of the given String
    bool is_end(const String& s, const CharIterator& i) const
    { return (i >= reinterpret_cast<CharIterator>(s->data()) + s->size()); }

    //! Return complete string (for debugging purposes)
    std::string get_string(const String& s, size_t depth = 0) const
    { return s->substr(depth); }

    //! Subset this string set using iterator range.
    UPtrStdStringSet sub(Iterator begin, Iterator end) const
    { return UPtrStdStringSet(begin, end); }

    //! Allocate a new temporary string container with n empty Strings
    static Container allocate(size_t n)
    { return std::make_pair(new String[n], n); }

    //! Deallocate a temporary string container
    static void deallocate(Container& c)
    { delete[] c.first; c.first = nullptr; }

    void print() const {
        size_t i = 0;
        for (Iterator pi = begin(); pi != end(); ++pi)
        {
            TLX_LOG1 << "[" << i++ << "] = " << pi->get()
                     << " = " << get_string(*pi, 0);
        }
    }

protected:
    //! vector of std::string objects
    Iterator begin_, end_;
};

/******************************************************************************/

/*!
 * Class implementing StringSet concept for suffix sorting indexes of a
 * std::string text object.
 */
class StringSuffixSetTraits
{
public:
    //! exported alias for assumed string container
    typedef std::string Text;

    //! exported alias for character type
    typedef std::uint8_t Char;

    //! String reference: suffix index of the text.
    typedef typename Text::size_type String;

    //! Iterator over string references: using std::vector's iterator over
    //! suffix array vector
    typedef typename std::vector<String>::iterator Iterator;

    //! iterator of characters in a string
    typedef const Char* CharIterator;

    //! exported alias for assumed string container
    typedef std::pair<Text, std::vector<String> > Container;
};

/*!
 * Class implementing StringSet concept for suffix sorting indexes of a
 * std::string text object.
 */
class StringSuffixSet
    : public StringSuffixSetTraits,
      public StringSetBase<StringSuffixSet, StringSuffixSetTraits>
{
public:
    //! Construct from begin and end string pointers
    StringSuffixSet(const Text& text,
                    const Iterator& begin, const Iterator& end)
        : text_(&text),
          begin_(begin), end_(end)
    { }

    //! Initializing constructor which fills output vector sa with indices.
    static StringSuffixSet
    Initialize(const Text& text, std::vector<String>& sa) {
        sa.resize(text.size());
        for (size_t i = 0; i < text.size(); ++i)
            sa[i] = i;
        return StringSuffixSet(text, sa.begin(), sa.end());
    }

    //! Return size of string array
    size_t size() const { return end_ - begin_; }
    //! Iterator representing first String position
    Iterator begin() const { return begin_; }
    //! Iterator representing beyond last String position
    Iterator end() const { return end_; }

    //! Array access (readable and writable) to String objects.
    String& operator [] (const Iterator& i) const
    { return *i; }

    //! Return CharIterator for referenced string, which belongs to this set.
    CharIterator get_chars(const String& s, size_t depth) const
    { return reinterpret_cast<CharIterator>(text_->data()) + s + depth; }

    //! Returns true if CharIterator is at end of the given String
    bool is_end(const String&, const CharIterator& i) const
    { return (i >= reinterpret_cast<CharIterator>(text_->data()) + text_->size()); }

    //! Return complete string (for debugging purposes)
    std::string get_string(const String& s, size_t depth = 0) const
    { return text_->substr(s + depth); }

    //! Subset this string set using iterator range.
    StringSuffixSet sub(Iterator begin, Iterator end) const
    { return StringSuffixSet(*text_, begin, end); }

    //! Allocate a new temporary string container with n empty Strings
    Container allocate(size_t n) const
    { return std::make_pair(*text_, std::vector<String>(n)); }

    //! Deallocate a temporary string container
    static void deallocate(Container& c)
    { std::vector<String> v; v.swap(c.second); }

    //! Construct from a string container
    explicit StringSuffixSet(Container& c)
        : text_(&c.first),
          begin_(c.second.begin()), end_(c.second.end())
    { }

protected:
    //! reference to base text
    const Text* text_;

    //! iterators inside the output suffix array.
    Iterator begin_, end_;
};

/******************************************************************************/

} // namespace sort_strings_detail

//! \}

} // namespace tlx

#endif // !TLX_SORT_STRINGS_STRING_SET_HEADER

/******************************************************************************/
