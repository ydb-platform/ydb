#pragma once

#include <util/system/defaults.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/mapfindptr.h>
#include <util/ysaveload.h>
#include <utility>

#include <initializer_list>

namespace NSorted {
    namespace NPrivate {
        template <class TPredicate>
        struct TEqual {
            template<typename TValueType1, typename TValueType2>
            inline bool operator()(const TValueType1& l, const TValueType2& r) const {
                TPredicate comp;
                return comp(l, r) == comp(r, l);
            }
        };

        template <typename TValueType, class TPredicate, class TKeyExtractor>
        struct TKeyCompare {
            inline bool operator()(const TValueType& l, const TValueType& r) const {
                TKeyExtractor extractKey;
                return TPredicate()(extractKey(l), extractKey(r));
            }
            template<typename TKeyType>
            inline bool operator()(const TKeyType& l, const TValueType& r) const {
                return TPredicate()(l, TKeyExtractor()(r));
            }
            template<typename TKeyType>
            inline bool operator()(const TValueType& l, const TKeyType& r) const {
                return TPredicate()(TKeyExtractor()(l), r);
            }
        };

        template <typename TValueType, class TPredicate>
        struct TKeyCompare<TValueType, TPredicate, TIdentity> {
            template <typename TValueType1, typename TValueType2>
            inline bool operator()(const TValueType1& l, const TValueType2& r) const {
                return TPredicate()(l, r);
            }
        };

    }

    // Sorted vector, which is order by the key. The key is extracted from the value by the provided key-extractor
    template <typename TValueType, typename TKeyType = TValueType, class TKeyExtractor = TIdentity,
              class TPredicate = TLess<TKeyType>, class A = std::allocator<TValueType>>
    class TSortedVector: public TVector<TValueType, A> {
    private:
        typedef TVector<TValueType, A> TBase;
        typedef NPrivate::TKeyCompare<TValueType, TPredicate, TKeyExtractor> TKeyCompare;
        typedef NPrivate::TEqual<TKeyCompare> TValueEqual;
        typedef NPrivate::TEqual<TPredicate> TKeyEqual;

    public:
        typedef TValueType value_type;
        typedef TKeyType key_type;
        typedef typename TBase::iterator iterator;
        typedef typename TBase::const_iterator const_iterator;
        typedef typename TBase::size_type size_type;

    public:
        inline TSortedVector()
            : TBase()
        {
        }

        inline explicit TSortedVector(size_type count)
            : TBase(count)
        {
        }

        inline TSortedVector(size_type count, const value_type& val)
            : TBase(count, val)
        {
        }

        inline TSortedVector(std::initializer_list<value_type> il)
            : TBase(il)
        {
            Sort();
        }

        inline TSortedVector(std::initializer_list<value_type> il, const typename TBase::allocator_type& a)
            : TBase(il, a)
        {
            Sort();
        }

        template <class TIter>
        inline TSortedVector(TIter first, TIter last)
            : TBase(first, last)
        {
            Sort();
        }

        // Inserts non-unique value in the proper position according to the key-sort order.
        // Returns iterator, which points to the inserted value
        inline iterator Insert(const value_type& value) {
            return TBase::insert(LowerBound(TKeyExtractor()(value)), value);
        }

        // STL-compatible synonym
        Y_FORCE_INLINE iterator insert(const value_type& value) {
            return this->Insert(value);
        }

        // Inserts non-unique value range in the proper position according to the key-sort order.
        template <class TIter>
        inline void Insert(TIter first, TIter last) {
            TBase::insert(TBase::end(), first, last);
            Sort();
        }

        // STL-compatible synonym
        template <class TIter>
        Y_FORCE_INLINE void insert(TIter first, TIter last) {
            this->Insert(first, last);
        }

        // Inserts unique value in the proper position according to the key-sort order,
        // if the value with the same key doesn't exist. Returns <iterator, bool> pair,
        // where the first member is the pointer to the inserted/existing value, and the
        // second member indicates either the value is inserted or not.
        inline std::pair<iterator, bool> InsertUnique(const value_type& value) {
            iterator i = LowerBound(TKeyExtractor()(value));
            if (i == TBase::end() || !TValueEqual()(*i, value))
                return std::pair<iterator, bool>(TBase::insert(i, value), true);
            else
                return std::pair<iterator, bool>(i, false);
        }

        // STL-compatible synonym
        Y_FORCE_INLINE std::pair<iterator, bool> insert_unique(const value_type& value) {
            return this->InsertUnique(value);
        }

        // Inserts unique value range in the proper position according to the key-sort order.
        template <class TIter>
        inline void InsertUnique(TIter first, TIter last) {
            TBase::insert(TBase::end(), first, last);
            Sort();
            MakeUnique();
        }

        // STL-compatible synonym
        template <class TIter>
        Y_FORCE_INLINE void insert_unique(TIter first, TIter last) {
            this->InsertUnique(first, last);
        }

        // Inserts unique value in the proper position according to the key-sort order.
        // If the value with the same key already exists, then it is replaced with the new one.
        // Returns iterator, which points to the inserted value
        inline iterator InsertOrReplace(const value_type& value) {
            iterator i = ::LowerBound(TBase::begin(), TBase::end(), value, TKeyCompare());
            if (i == TBase::end() || !TValueEqual()(*i, value))
                return TBase::insert(i, value);
            else
                return TBase::insert(TBase::erase(i), value);
        }

        // STL-compatible synonym
        Y_FORCE_INLINE iterator insert_or_replace(const value_type& value) {
            return this->InsertOrReplace(value);
        }

        Y_FORCE_INLINE void Sort() {
            ::Sort(TBase::begin(), TBase::end(), TKeyCompare());
        }

        // STL-compatible synonym
        Y_FORCE_INLINE void sort() {
            this->Sort();
        }

        Y_FORCE_INLINE void Sort(iterator from, iterator to) {
            ::Sort(from, to, TKeyCompare());
        }

        // STL-compatible synonym
        Y_FORCE_INLINE void sort(iterator from, iterator to) {
            this->Sort(from, to);
        }

        inline void MakeUnique() {
            TBase::erase(::Unique(TBase::begin(), TBase::end(), TValueEqual()), TBase::end());
        }

        // STL-compatible synonym
        Y_FORCE_INLINE void make_unique() {
            this->MakeUnique();
        }

        template<class K>
        inline const_iterator Find(const K& key) const {
            const_iterator i = LowerBound(key);
            if (i == TBase::end() || !TKeyEqual()(TKeyExtractor()(*i), key))
                return TBase::end();
            else
                return i;
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE const_iterator find(const K& key) const {
            return this->Find(key);
        }

        template<class K>
        inline iterator Find(const K& key) {
            iterator i = LowerBound(key);
            if (i == TBase::end() || !TKeyEqual()(TKeyExtractor()(*i), key))
                return TBase::end();
            else
                return i;
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE iterator find(const K& key) {
            return this->Find(key);
        }

        template<class K>
        Y_FORCE_INLINE bool Has(const K& key) const {
            return this->find(key) != TBase::end();
        }

        template<class K>
        Y_FORCE_INLINE bool has(const K& key) const {
            return this->Has(key);
        }

        template<class K>
        Y_FORCE_INLINE bool contains(const K& key) const {
            return this->Has(key);
        }

        template<class K>
        Y_FORCE_INLINE iterator LowerBound(const K& key) {
            return ::LowerBound(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE iterator lower_bound(const K& key) {
            return this->LowerBound(key);
        }

        template<class K>
        Y_FORCE_INLINE const_iterator LowerBound(const K& key) const {
            return ::LowerBound(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE const_iterator lower_bound(const K& key) const {
            return this->LowerBound(key);
        }

        template<class K>
        Y_FORCE_INLINE iterator UpperBound(const K& key) {
            return ::UpperBound(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE iterator upper_bound(const K& key) {
            return this->UpperBound(key);
        }

        template<class K>
        Y_FORCE_INLINE const_iterator UpperBound(const K& key) const {
            return ::UpperBound(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE const_iterator upper_bound(const K& key) const {
            return this->UpperBound(key);
        }

        template<class K>
        Y_FORCE_INLINE std::pair<iterator, iterator> EqualRange(const K& key) {
            return std::equal_range(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE std::pair<iterator, iterator> equal_range(const K& key) {
            return this->EqualRange(key);
        }

        template<class K>
        Y_FORCE_INLINE std::pair<const_iterator, const_iterator> EqualRange(const K& key) const {
            return std::equal_range(TBase::begin(), TBase::end(), key, TKeyCompare());
        }

        // STL-compatible synonym
        template<class K>
        Y_FORCE_INLINE std::pair<const_iterator, const_iterator> equal_range(const K& key) const {
            return this->EqualRange(key);
        }

        template<class K>
        inline void Erase(const K& key) {
            std::pair<iterator, iterator> res = EqualRange(key);
            TBase::erase(res.first, res.second);
        }

        // STL-compatible synonym
        Y_FORCE_INLINE void erase(const key_type& key) {
            this->Erase(key);
        }

        template<class K>
        inline size_t count(const K& key) const {
            const std::pair<const_iterator, const_iterator> range = this->EqualRange(key);
            return std::distance(range.first, range.second);
        }

        using TBase::erase;
    };

    // The simplified map (a.k.a TFlatMap, flat_map), which is implemented by the sorted-vector.
    // This structure has the side-effect: if you keep a reference to an existing element
    // and then inserts a new one, the existing reference can be broken (due to reallocation).
    // Please keep this in mind when using this structure.
    template <typename TKeyType, typename TValueType, class TPredicate = TLess<TKeyType>, class A = std::allocator<TValueType>>
    class TSimpleMap:
        public TSortedVector<std::pair<TKeyType, TValueType>, TKeyType, TSelect1st, TPredicate, A>,
        public TMapOps<TSimpleMap<TKeyType, TValueType, TPredicate, A>>
    {
    private:
        typedef TSortedVector<std::pair<TKeyType, TValueType>, TKeyType, TSelect1st, TPredicate, A> TBase;

    public:
        typedef typename TBase::value_type value_type;
        typedef typename TBase::key_type key_type;
        typedef typename TBase::iterator iterator;
        typedef typename TBase::const_iterator const_iterator;
        typedef typename TBase::size_type size_type;

    public:
        inline TSimpleMap()
            : TBase()
        {
        }

        template <class TIter>
        inline TSimpleMap(TIter first, TIter last)
            : TBase(first, last)
        {
            TBase::MakeUnique();
        }

        inline TSimpleMap(std::initializer_list<value_type> il)
            : TBase(il)
        {
            TBase::MakeUnique();
        }

        inline TValueType& Get(const TKeyType& key) {
            typename TBase::iterator i = TBase::LowerBound(key);
            if (i == TBase::end() || key != i->first)
                return TVector<std::pair<TKeyType, TValueType>, A>::insert(i, std::make_pair(key, TValueType()))->second;
            else
                return i->second;
        }

        template<class K>
        inline const TValueType& Get(const K& key, const TValueType& def) const {
            typename TBase::const_iterator i = TBase::Find(key);
            return i != TBase::end() ? i->second : def;
        }

        template<class K>
        Y_FORCE_INLINE TValueType& operator[](const K& key) {
            return Get(key);
        }

        template<class K>
        const TValueType& at(const K& key) const {
            const auto i = TBase::Find(key);
            if (i == TBase::end()) {
                throw std::out_of_range("NSorted::TSimpleMap: missing key");
            }

            return i->second;
        }

        template<class K>
        TValueType& at(const K& key) {
            return const_cast<TValueType&>(
                    const_cast<const TSimpleMap<TKeyType, TValueType, TPredicate, A>*>(this)->at(key));
        }
    };

    // The simplified set (a.k.a TFlatSet, flat_set), which is implemented by the sorted-vector.
    // This structure has the same side-effect as TSimpleMap.
    // The value type must have TValueType(TKeyType) constructor in order to use [] operator
    template <typename TValueType, typename TKeyType = TValueType, class TKeyExtractor = TIdentity,
              class TPredicate = TLess<TKeyType>, class A = std::allocator<TValueType>>
    class TSimpleSet: public TSortedVector<TValueType, TKeyType, TKeyExtractor, TPredicate, A> {
    private:
        typedef TSortedVector<TValueType, TKeyType, TKeyExtractor, TPredicate, A> TBase;

    public:
        typedef typename TBase::value_type value_type;
        typedef typename TBase::key_type key_type;
        typedef typename TBase::iterator iterator;
        typedef typename TBase::const_iterator const_iterator;
        typedef typename TBase::size_type size_type;
        typedef NPrivate::TEqual<TPredicate> TKeyEqual;

    public:
        inline TSimpleSet()
            : TBase()
        {
        }

        template <class TIter>
        inline TSimpleSet(TIter first, TIter last)
            : TBase(first, last)
        {
            TBase::MakeUnique();
        }

        inline TSimpleSet(std::initializer_list<value_type> il)
            : TBase(il)
        {
            TBase::MakeUnique();
        }

        // The method expects that there is a TValueType(TKeyType) constructor available
        inline TValueType& Get(const TKeyType& key) {
            typename TBase::iterator i = TBase::LowerBound(key);
            if (i == TBase::end() || !TKeyEqual()(TKeyExtractor()(*i), key))
                i = TVector<TValueType, A>::insert(i, TValueType(key));
            return *i;
        }

        template<class K>
        inline const TValueType& Get(const K& key, const TValueType& def) const {
            typename TBase::const_iterator i = TBase::Find(key);
            return i != TBase::end() ? *i : def;
        }

        template<class K>
        Y_FORCE_INLINE TValueType& operator[](const K& key) {
            return Get(key);
        }

        // Inserts value with unique key. Returns <iterator, bool> pair,
        // where the first member is the pointer to the inserted/existing value, and the
        // second member indicates either the value is inserted or not.
        Y_FORCE_INLINE std::pair<iterator, bool> Insert(const TValueType& value) {
            return TBase::InsertUnique(value);
        }

        // STL-compatible synonym
        Y_FORCE_INLINE std::pair<iterator, bool> insert(const TValueType& value) {
            return TBase::InsertUnique(value);
        }

        // Inserts value range with unique keys.
        template <class TIter>
        Y_FORCE_INLINE void Insert(TIter first, TIter last) {
            TBase::InsertUnique(first, last);
        }

        // STL-compatible synonym
        template <class TIter>
        Y_FORCE_INLINE void insert(TIter first, TIter last) {
            TBase::InsertUnique(first, last);
        }
    };

}

template <typename V, typename K, class E, class P, class A>
class TSerializer<NSorted::TSortedVector<V, K, E, P, A>>: public TVectorSerializer<NSorted::TSortedVector<V, K, E, P, A>> {
};

template <typename K, typename V, class P, class A>
class TSerializer<NSorted::TSimpleMap<K, V, P, A>>: public TVectorSerializer<NSorted::TSimpleMap<K, V, P, A>> {
};

template <typename V, typename K, class E, class P, class A>
class TSerializer<NSorted::TSimpleSet<V, K, E, P, A>>: public TVectorSerializer<NSorted::TSimpleSet<V, K, E, P, A>> {
};
