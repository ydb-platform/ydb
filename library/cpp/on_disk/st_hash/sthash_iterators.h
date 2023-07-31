#pragma once

#include "save_stl.h"

#include <util/system/align.h>

/**
    This file provides functionality for saving some relatively simple THashMap object
    to disk in a form that can be mapped read-only (via mmap) at any address.
    That saved object is accessed via pointer to sthash object (that must have
    the same parameters as original THashMap object)

    If either key or value are variable-sized (i.e. contain pointers), user must
    write his own instantiation of TSthashIterator (read iterator for sthash) and
    TSthashWriter (write iterator for THashMap).
    An example for <const char *, B> pair is in here.
**/

// TEmptyValue and SizeOfEx are helpers for sthash_set
struct TEmptyValue {
    TEmptyValue() = default;
};

template <class T>
inline size_t SizeOfEx() {
    return sizeof(T);
}

template <>
inline size_t SizeOfEx<TEmptyValue>() {
    return 0;
}
template <>
inline size_t SizeOfEx<const TEmptyValue>() {
    return 0;
}

template <class TKey, class TValue, class HashFcn, class EqualKey>
struct TSthashIterator {
    // Implementation for simple types
    typedef const TKey TKeyType;
    typedef const TValue TValueType;
    typedef EqualKey TKeyEqualType;
    typedef HashFcn THasherType;

    const char* Data;
    TSthashIterator()
        : Data(nullptr)
    {
    }
    explicit TSthashIterator(const char* data)
        : Data(data)
    {
    }
    void operator++() {
        Data += GetLength();
    }

    bool operator!=(const TSthashIterator& that) const {
        return Data != that.Data;
    }
    bool operator==(const TSthashIterator& that) const {
        return Data == that.Data;
    }
    TKey& Key() const {
        return *(TKey*)Data;
    }
    TValue& Value() {
        return *(TValue*)(Data + sizeof(TKey));
    }
    const TValue& Value() const {
        return *(const TValue*)(Data + sizeof(TKey));
    }

    template <class AnotherKeyType>
    bool KeyEquals(const EqualKey& eq, const AnotherKeyType& key) const {
        return eq(*(TKey*)Data, key);
    }

    size_t GetLength() const {
        return sizeof(TKey) + SizeOfEx<TValue>();
    }
};

template <class Key, class Value, typename size_type_o = ui64>
struct TSthashWriter {
    typedef size_type_o TSizeType;
    size_t GetRecordSize(const std::pair<const Key, const Value>&) const {
        return sizeof(Key) + SizeOfEx<Value>();
    }
    int SaveRecord(IOutputStream* stream, const std::pair<const Key, const Value>& record) const {
        stream->Write(&record.first, sizeof(Key));
        stream->Write(&record.second, SizeOfEx<Value>());
        return 0;
    }
};

// Remember that this simplified implementation makes a copy of `key' in std::make_pair.
// It can also waste some memory on undesired alignment.
template <class Key, typename size_type_o = ui64>
struct TSthashSetWriter: public TSthashWriter<Key, TEmptyValue, size_type_o> {
    typedef TSthashWriter<Key, TEmptyValue, size_type_o> MapWriter;
    size_t GetRecordSize(const Key& key) const {
        return MapWriter::GetRecordSize(std::make_pair(key, TEmptyValue()));
    }
    int SaveRecord(IOutputStream* stream, const Key& key) const {
        return MapWriter::SaveRecord(stream, std::make_pair(key, TEmptyValue()));
    }
};

// we can't save something with pointers without additional tricks

template <class A, class B, class HashFcn, class EqualKey>
struct TSthashIterator<A*, B, HashFcn, EqualKey> {};

template <class A, class B, class HashFcn, class EqualKey>
struct TSthashIterator<A, B*, HashFcn, EqualKey> {};

template <class A, class B, typename size_type_o>
struct TSthashWriter<A*, B*, size_type_o> {};

template <class A, class B, typename size_type_o>
struct TSthashWriter<A*, B, size_type_o> {};

template <class A, class B, typename size_type_o>
struct TSthashWriter<A, B*, size_type_o> {};

template <class T>
inline size_t AlignForChrKey() {
    return 4; // TODO: change this (requeres rebuilt of a few existing files)
}

template <>
inline size_t AlignForChrKey<TEmptyValue>() {
    return 1;
}

template <>
inline size_t AlignForChrKey<const TEmptyValue>() {
    return AlignForChrKey<TEmptyValue>();
}

// !! note that for char*, physical placement of key and value is swapped
template <class TValue, class HashFcn, class EqualKey>
struct TSthashIterator<const char* const, TValue, HashFcn, EqualKey> {
    typedef const TValue TValueType;
    typedef const char* TKeyType;
    typedef EqualKey TKeyEqualType;
    typedef HashFcn THasherType;

    const char* Data;
    TSthashIterator()
        : Data(nullptr)
    {
    }
    TSthashIterator(const char* data)
        : Data(data)
    {
    }
    void operator++() {
        Data += GetLength();
    }

    bool operator!=(const TSthashIterator& that) const {
        return Data != that.Data;
    }
    bool operator==(const TSthashIterator& that) const {
        return Data == that.Data;
    }
    const char* Key() const {
        return Data + SizeOfEx<TValue>();
    }
    TValue& Value() {
        return *(TValue*)Data;
    }
    const TValue& Value() const {
        return *(const TValue*)Data;
    }

    template <class K>
    bool KeyEquals(const EqualKey& eq, const K& k) const {
        return eq(Data + SizeOfEx<TValue>(), k);
    }

    size_t GetLength() const {
        size_t length = strlen(Data + SizeOfEx<TValue>()) + 1 + SizeOfEx<TValue>();
        length = AlignUp(length, AlignForChrKey<TValue>());
        return length;
    }
};

template <class Value, typename size_type_o>
struct TSthashWriter<const char*, Value, size_type_o> {
    typedef size_type_o TSizeType;
    size_t GetRecordSize(const std::pair<const char*, const Value>& record) const {
        size_t length = strlen(record.first) + 1 + SizeOfEx<Value>();
        length = AlignUp(length, AlignForChrKey<Value>());
        return length;
    }
    int SaveRecord(IOutputStream* stream, const std::pair<const char*, const Value>& record) const {
        const char* alignBuffer = "qqqq";
        stream->Write(&record.second, SizeOfEx<Value>());
        size_t length = strlen(record.first) + 1;
        stream->Write(record.first, length);
        length = AlignUpSpace(length, AlignForChrKey<Value>());
        if (length)
            stream->Write(alignBuffer, length);
        return 0;
    }
};

template <class TKey, class HashFcn, class EqualKey>
struct TSthashIterator<TKey, const char* const, HashFcn, EqualKey> {
    typedef const TKey TKeyType;
    typedef const char* TValueType;
    typedef EqualKey TKeyEqualType;
    typedef HashFcn THasherType;

    const char* Data;
    TSthashIterator()
        : Data(nullptr)
    {
    }
    TSthashIterator(const char* data)
        : Data(data)
    {
    }
    void operator++() {
        Data += GetLength();
    }

    bool operator!=(const TSthashIterator& that) const {
        return Data != that.Data;
    }
    bool operator==(const TSthashIterator& that) const {
        return Data == that.Data;
    }
    TKey& Key() {
        return *(TKey*)Data;
    }
    const char* Value() const {
        return Data + sizeof(TKey);
    }

    template <class K>
    bool KeyEquals(const EqualKey& eq, const K& k) const {
        return eq(*(TKey*)Data, k);
    }

    size_t GetLength() const {
        size_t length = strlen(Data + sizeof(TKey)) + 1 + sizeof(TKey);
        length = AlignUp(length, (size_t)4);
        return length;
    }
};

template <class Key, typename size_type_o>
struct TSthashWriter<Key, const char*, size_type_o> {
    typedef size_type_o TSizeType;
    size_t GetRecordSize(const std::pair<const Key, const char*>& record) const {
        size_t length = strlen(record.second) + 1 + sizeof(Key);
        length = AlignUp(length, (size_t)4);
        return length;
    }
    int SaveRecord(IOutputStream* stream, const std::pair<const Key, const char*>& record) const {
        const char* alignBuffer = "qqqq";
        stream->Write(&record.first, sizeof(Key));
        size_t length = strlen(record.second) + 1;
        stream->Write(record.second, length);
        length = AlignUpSpace(length, (size_t)4);
        if (length)
            stream->Write(alignBuffer, length);
        return 0;
    }
};

template <class HashFcn, class EqualKey>
struct TSthashIterator<const char* const, const char* const, HashFcn, EqualKey> {
    typedef const char* TKeyType;
    typedef const char* TValueType;
    typedef EqualKey TKeyEqualType;
    typedef HashFcn THasherType;

    const char* Data;
    TSthashIterator()
        : Data(nullptr)
    {
    }
    TSthashIterator(const char* data)
        : Data(data)
    {
    }
    void operator++() {
        Data += GetLength();
    }

    bool operator!=(const TSthashIterator& that) const {
        return Data != that.Data;
    }
    bool operator==(const TSthashIterator& that) const {
        return Data == that.Data;
    }
    const char* Key() const {
        return Data;
    }
    const char* Value() const {
        return Data + strlen(Data) + 1;
    }

    template <class K>
    bool KeyEquals(const EqualKey& eq, const K& k) const {
        return eq(Data, k);
    }

    size_t GetLength() const {
        size_t length = strlen(Data) + 1;
        length += strlen(Data + length) + 1;
        return length;
    }
};

template <typename size_type_o>
struct TSthashWriter<const char*, const char*, size_type_o> {
    typedef size_type_o TSizeType;
    size_t GetRecordSize(const std::pair<const char*, const char*>& record) const {
        size_t size = strlen(record.first) + strlen(record.second) + 2;
        return size;
    }
    int SaveRecord(IOutputStream* stream, const std::pair<const char*, const char*>& record) const {
        stream->Write(record.first, strlen(record.first) + 1);
        stream->Write(record.second, strlen(record.second) + 1);
        return 0;
    }
};
