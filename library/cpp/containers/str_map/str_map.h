#pragma once

#include <util/memory/segmented_string_pool.h>
#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/generic/buffer.h>
#include <util/str_stl.h> // less<> and equal_to<> for const char*
#include <utility>
#include <util/generic/noncopyable.h>

template <class T, class HashFcn = THash<const char*>, class EqualTo = TEqualTo<const char*>, class Alloc = std::allocator<const char*>>
class string_hash;

template <class T, class HashFcn = THash<const char*>, class EqualTo = TEqualTo<const char*>>
class segmented_string_hash;

template <class Map>
inline std::pair<typename Map::iterator, bool>
pool_insert(Map* m, const char* key, const typename Map::mapped_type& data, TBuffer& pool) {
    std::pair<typename Map::iterator, bool> ins = m->insert(typename Map::value_type(key, data));
    if (ins.second) {                    // new?
        size_t buflen = strlen(key) + 1; // strlen???
        const char* old_pool = pool.Begin();
        pool.Append(key, buflen);
        if (pool.Begin() != old_pool) // repoint?
            for (typename Map::iterator it = m->begin(); it != m->end(); ++it)
                if ((*it).first != key)
                    const_cast<const char*&>((*it).first) += (pool.Begin() - old_pool);
        const_cast<const char*&>((*ins.first).first) = pool.End() - buflen;
    }
    return ins;
}

#define HASH_SIZE_DEFAULT 100
#define AVERAGEWORD_BUF 10

template <class T, class HashFcn, class EqualTo, class Alloc>
class string_hash: public THashMap<const char*, T, HashFcn, EqualTo, Alloc> {
protected:
    TBuffer pool;

public:
    using yh = THashMap<const char*, T, HashFcn, EqualTo, Alloc>;
    using iterator = typename yh::iterator;
    using const_iterator = typename yh::const_iterator;
    using mapped_type = typename yh::mapped_type;
    using size_type = typename yh::size_type;
    using pool_size_type = typename yh::size_type;
    string_hash() {
        pool.Reserve(HASH_SIZE_DEFAULT * AVERAGEWORD_BUF); // reserve here
    }
    string_hash(size_type hash_size, pool_size_type pool_size)
        : THashMap<const char*, T, HashFcn, EqualTo, Alloc>(hash_size)
    {
        pool.Reserve(pool_size); // reserve here
    }

    std::pair<iterator, bool> insert_copy(const char* key, const mapped_type& data) {
        return ::pool_insert(this, key, data, pool);
    }

    void clear_hash() {
        yh::clear();
        pool.Clear();
    }
    pool_size_type pool_size() const {
        return pool.Size();
    }

    string_hash(const string_hash& sh)
        : THashMap<const char*, T, HashFcn, EqualTo, Alloc>()
    {
        for (const_iterator i = sh.begin(); i != sh.end(); ++i)
            insert_copy((*i).first, (*i).second);
    }
    /* May be faster?
    string_hash(const string_hash& sh)
        : THashMap<const char *, T, HashFcn, EqualTo>(sh)
    {
        pool = sh.pool;
        size_t delta = pool.begin() - sh.pool.begin();
        for (iterator i = begin(); i != end(); ++i)
            (const char*&)(*i).first += delta;
    }
    */
    string_hash& operator=(const string_hash& sh) {
        if (&sh != this) {
            clear_hash();
            for (const_iterator i = sh.begin(); i != sh.end(); ++i)
                insert_copy((*i).first, (*i).second);
        }
        return *this;
    }

    mapped_type& operator[](const char* key) {
        iterator I = yh::find(key);
        if (I == yh::end())
            I = insert_copy(key, mapped_type()).first;
        return (*I).second;
    }
};

template <class C, class T, class HashFcn, class EqualTo>
class THashWithSegmentedPoolForKeys: protected  THashMap<const C*, T, HashFcn, EqualTo>, TNonCopyable {
protected:
    segmented_pool<C> pool;

public:
    using yh = THashMap<const C*, T, HashFcn, EqualTo>;
    using iterator = typename yh::iterator;
    using const_iterator = typename yh::const_iterator;
    using mapped_type = typename yh::mapped_type;
    using size_type = typename yh::size_type;
    using key_type = typename yh::key_type;
    using value_type = typename yh::value_type;

    THashWithSegmentedPoolForKeys(size_type hash_size = HASH_SIZE_DEFAULT, size_t segsize = HASH_SIZE_DEFAULT * AVERAGEWORD_BUF, bool afs = false)
        : yh(hash_size)
        , pool(segsize)
    {
        if (afs)
            pool.alloc_first_seg();
    }

    std::pair<iterator, bool> insert_copy(const C* key, size_t keylen, const mapped_type& data) {
        std::pair<iterator, bool> ins = this->insert(value_type(key, data));
        if (ins.second) // new?
            (const C*&)(*ins.first).first = pool.append(key, keylen);
        return ins;
    }

    void clear_hash() {
        yh::clear();
        pool.restart();
    }

    size_t pool_size() const {
        return pool.size();
    }

    size_t size() const {
        return yh::size();
    }

    bool empty() const {
        return yh::empty();
    }

    iterator begin() {
        return yh::begin();
    }

    iterator end() {
        return yh::end();
    }

    const_iterator begin() const {
        return yh::begin();
    }

    const_iterator end() const {
        return yh::end();
    }

    iterator find(const key_type& key) {
        return yh::find(key);
    }

    const_iterator find(const key_type& key) const {
        return yh::find(key);
    }

    const yh& get_THashMap() const {
        return static_cast<const yh&>(*this);
    }
};

template <class T, class HashFcn, class EqualTo>
class segmented_string_hash: public THashWithSegmentedPoolForKeys<char, T, HashFcn, EqualTo> {
public:
    using Base = THashWithSegmentedPoolForKeys<char, T, HashFcn, EqualTo>;
    using iterator = typename Base::iterator;
    using const_iterator = typename Base::const_iterator;
    using mapped_type = typename Base::mapped_type;
    using size_type = typename Base::size_type;
    using key_type = typename Base::key_type;
    using value_type = typename Base::value_type;

public:
    segmented_string_hash(size_type hash_size = HASH_SIZE_DEFAULT, size_t segsize = HASH_SIZE_DEFAULT * AVERAGEWORD_BUF, bool afs = false)
        : Base(hash_size, segsize, afs)
    {
    }

    std::pair<iterator, bool> insert_copy(const char* key, const mapped_type& data) {
        return Base::insert_copy(key, strlen(key) + 1, data);
    }

    mapped_type& operator[](const char* key) {
        iterator I = Base::find(key);
        if (I == Base::end())
            I = insert_copy(key, mapped_type()).first;
        return (*I).second;
    }
};
