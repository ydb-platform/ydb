#pragma once

#include <library/cpp/digest/lower_case/lchash.h>

#include <util/generic/hash_multi_map.h>
#include <util/generic/strbuf.h>
#include <util/generic/algorithm.h>
#include <util/generic/singleton.h>

struct TStrBufHash {
    inline size_t operator()(const TStringBuf& s) const noexcept {
        return FnvCaseLess<size_t>(s);
    }
};

struct TStrBufEqualToCaseLess {
    inline bool operator()(const TStringBuf& c1, const TStringBuf& c2) const noexcept {
        typedef TLowerCaseIterator<const TStringBuf::TChar> TIter;

        return (c1.size() == c2.size()) && std::equal(TIter(c1.begin()), TIter(c1.end()), TIter(c2.begin()));
    }
};

template <class T>
class TLowerCaseTable: private THashMultiMap<TStringBuf, T, TStrBufHash, TStrBufEqualToCaseLess> {
    typedef THashMultiMap<TStringBuf, T, TStrBufHash, TStrBufEqualToCaseLess> TBase;

public:
    typedef typename TBase::const_iterator const_iterator;
    typedef std::pair<const_iterator, const_iterator> TConstIteratorPair;

    using TBase::TBase;
    using TBase::begin;
    using TBase::end;

    inline TConstIteratorPair EqualRange(const TStringBuf& name) const {
        return TBase::equal_range(name);
    }

    inline const T& Get(const TStringBuf& name, size_t numOfValue = 0) const {
        TConstIteratorPair range = EqualRange(name);

        if (range.first == TBase::end())
            return Default<T>();

        if (numOfValue == 0)
            return range.first->second;

        const_iterator next = range.first;
        for (size_t c = 0; c < numOfValue; ++c) {
            ++next;
            if (next == range.second)
                return Default<T>();
        }

        return next->second;
    }

    inline bool Has(const TStringBuf& name) const {
        return TBase::find(name) != TBase::end();
    }

    size_t NumOfValues(const TStringBuf& name) const {
        return TBase::count(name);
    }

    inline size_t Size() const noexcept {
        return TBase::size();
    }

    inline bool Empty() const noexcept {
        return TBase::empty();
    }

    inline void Add(const TStringBuf& key, const T& val) {
        TBase::insert(typename TBase::value_type(key, val));
    }

    inline void Clear() noexcept {
        TBase::clear();
    }

    inline size_t Erase(const TStringBuf& key) {
        return TBase::erase(key);
    }
};
