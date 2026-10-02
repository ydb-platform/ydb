#pragma once

#include <list>
#include <unordered_map>

namespace NKikimr {

// Container which stores unique elements in FIFO-order and allows:
// O(1) Front(), Back()
// O(1) PushBack(value), PopBack(), ExtractBack()
// O(1) PushFront(value), PopFront(), ExtractFront()
// O(1) Contains(value), Erase(value)
// O(1) Size()
// When pushing value already present in container, old value will be erased.

template<typename T>
class TFastLookupUniqueList {
private:
    using TList = std::list<T>;
    using TLookup = std::unordered_map<T, typename TList::iterator>;

public:
    using TIterator = TList::iterator;
    using TConstIterator = TList::const_iterator;

public:
    TFastLookupUniqueList() = default;

    // move-assigning both containers preserves invariant
    TFastLookupUniqueList(TFastLookupUniqueList&&) = default;
    TFastLookupUniqueList& operator=(TFastLookupUniqueList&&) = default;

    // we mustn't copy Lookup
    TFastLookupUniqueList(const TFastLookupUniqueList& other) {
        for (const T& value : other) {
            PushBack(value);
        }
    }

    TFastLookupUniqueList& operator=(const TFastLookupUniqueList& other) {
        Clear();
        for (const T& value : other) {
            PushBack(value);
        }
        return *this;
    }

public:
    size_t Size() const {
        return Data.size();
    }

    TIterator begin() {
        return Data.begin();
    }

    TIterator end() {
        return Data.end();
    }

    TConstIterator begin() const {
        return Data.begin();
    }

    TConstIterator end() const {
        return Data.end();
    }

    const T& Back() const {
        return Data.back();
    }

    void PushBack(const T& value) {
        Erase(value);

        Data.push_back(value);
        Lookup[value] = --Data.end();
    }

    void PushBack(T&& value) {
        Erase(value);

        Data.push_back(std::forward<T>(value));
        Lookup[value] = --Data.end();
    }

    void PopBack() {
        Lookup.erase(Data.back());
        Data.pop_back();
    }

    T ExtractBack() {
        T value = Back();
        PopBack();
        return value;
    }

    const T& Front() const {
        return Data.front();
    }

    void PushFront(const T& value) {
        Erase(value);

        Data.push_front(value);
        Lookup[value] = Data.begin();
    }

    void PushFront(T&& value) {
        Erase(value);

        Data.push_front(std::forward<T>(value));
        Lookup[value] = Data.begin();
    }

    void PopFront() {
        Lookup.erase(Data.front());
        Data.pop_front();
    }

    T ExtractFront() {
        T value = Front();
        PopFront();
        return value;
    }

    bool Contains(const T& value) const {
        return Lookup.contains(value);
    }

    size_t Erase(const T& value) {
        if (!Contains(value)) {
            return 0;
        }

        TIterator it = Lookup.at(value);
        Data.erase(it);
        Lookup.erase(value);
        return 1;
    }

    bool IsEmpty() const {
        return Data.empty();
    }

    void Clear() {
        Data.clear();
        Lookup.clear();
    }

private:
    TList Data;
    TLookup Lookup;
};

} // namespace NKikimr
