#pragma once

#include <library/cpp/containers/str_map/str_map.h>
#include <library/cpp/charset/ci_string.h>
#include <util/system/yassert.h>
#include <util/memory/tempbuf.h>

#include <memory>

class IInputStream;
class IOutputStream;

template <class T, class Alloc = std::allocator<const char*>>
class Hash;

struct yvoid {
    yvoid() = default;
};

template <typename T, class Alloc>
class Hash: public string_hash<T, ci_hash, ci_equal_to, Alloc> {
    using ci_string_hash = string_hash<T, ci_hash, ci_equal_to, Alloc>;

protected:
    using ci_string_hash::pool;

public:
    using size_type = typename ci_string_hash::size_type;
    using const_iterator = typename ci_string_hash::const_iterator;
    using iterator = typename ci_string_hash::iterator;
    using value_type = typename ci_string_hash::value_type;
    using ci_string_hash::begin;
    using ci_string_hash::end;
    using ci_string_hash::find;
    using ci_string_hash::size;

    Hash()
        : ci_string_hash()
    {
    }
    explicit Hash(size_type theSize)
        : ci_string_hash(theSize, theSize * AVERAGEWORD_BUF)
    {
    }
    Hash(const char** strings, size_type size = 0, T* = 0); // must end with NULL or "\0"
    virtual ~Hash();
    bool Has(const char* s, size_t len, T* pp = nullptr) const;
    bool Has(const char* s, T* pp = nullptr) const {
        const_iterator it;
        if ((it = find(s)) == end())
            return false;
        else if (pp)
            *pp = (*it).second;
        return true;
    }
    void Add(const char* s, T data) {
        // in fact it is the same insert_unique as in AddUnique.
        // it's impossible to have _FAST_ version of insert() in 'hash_map'

        // you have to use 'hash_mmap' to get the _kind_ of desired effect.
        // BUT still there will be "Checks" inside -
        // to make the same keys close to each other (see insert_equal())
        this->insert_copy(s, data);
    }
    bool AddUniq(const char* s, T data) {
        return this->insert_copy(s, data).second;
    }
    // new function to get rid of allocations completely! -- e.g. in constructors
    void AddPermanent(const char* s, T data) {
        this->insert(value_type(s, data));
    }
    T Detach(const char* s) {
        iterator it = find(s);
        if (it == end())
            return T();
        T data = (*it).second;
        this->erase(it);
        return data;
    }
    size_type NumEntries() const {
        return size();
    }
    bool ForEach(bool (*func)(const char* key, T data, void* cookie), void* cookie = nullptr);
    void Resize(size_type theSize) {
        this->reserve(theSize);
        // no pool resizing here.
    }
    virtual void Clear();
    char* Pool() {
        if (pool.Size() < 2 || pool.End()[-2] != '\0')
            pool.Append("\0", 1);
        return pool.Begin();
    }
};

template <class T, class Alloc>
Hash<T, Alloc>::Hash(const char** array, size_type theSize, T* data) {
    // must end with NULL or "\0"
    Y_ASSERT(data != nullptr);
    Resize(theSize);
    while (*array && **array)
        AddPermanent(*array++, *data++);
}

template <class T, class Alloc>
bool Hash<T, Alloc>::Has(const char* s, size_t len, T* pp) const {
    TTempArray<char> buf(len + 1);
    char* const allocated = buf.Data();
    memcpy(allocated, s, len);
    allocated[len] = '\x00';
    return Has(allocated, pp);
}

template <class T, class Alloc>
Hash<T, Alloc>::~Hash() {
    Clear();
}

template <class T, class Alloc>
void Hash<T, Alloc>::Clear() {
    ci_string_hash::clear_hash(); // to make the key pool empty
}

template <class T, class Alloc>
bool Hash<T, Alloc>::ForEach(bool (*func)(const char* key, T data, void* cookie), void* cookie) {
    for (const_iterator it = begin(); it != end(); ++it)
        if (!func((*it).first, (*it).second, cookie))
            return false;
    return true;
}

class HashSet: public Hash<yvoid> {
public:
    HashSet(const char** array, size_type size = 0);
    HashSet()
        : Hash<yvoid>()
    {
    }
    void Read(IInputStream* input);
    void Write(IOutputStream* output) const;
    void Add(const char* s) {
        // in fact it is the same insert_unique as in AddUnique.
        // it's impossible to have _FAST_ version of insert() in 'hash_map'

        // you have to use 'hash_mmap' to get the _kind_ of desired effect.
        // BUT still there will be "Checks" inside -
        // to make the same keys close to each other (see insert_equal())
        insert_copy(s, yvoid());
    }
    bool AddUniq(const char* s) {
        return insert_copy(s, yvoid()).second;
    }
    // new function to get rid of allocations completely! -- e.g. in constructors
    void AddPermanent(const char* s) {
        insert(value_type(s, yvoid()));
    }
};

template <class T, class HashFcn = THash<T>, class EqualKey = TEqualTo<T>, class Alloc = std::allocator<T>>
class TStaticHash: private THashMap<T, T, HashFcn, EqualKey> {
private:
    using TBase = THashMap<T, T, HashFcn, EqualKey>;

public:
    TStaticHash(T arr[][2], size_t size) {
        TBase::reserve(size);
        while (size) {
            TBase::insert(typename TBase::value_type(arr[0][0], arr[0][1]));
            arr++;
            size--;
        }
    }
    T operator[](const T& key) const { // !!! it is not lvalue nor it used to be
        typename TBase::const_iterator it = TBase::find(key);
        if (it == TBase::end())
            return nullptr;
        return it->second;
    }
};

using TStHash = TStaticHash<const char*, ci_hash, ci_equal_to>;
