#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/noncopyable.h>
#include <util/generic/utility.h>
#include <util/system/yassert.h>

#include <stdlib.h>

template <typename T, class A = std::allocator<T>>
class TVectorSwaps : TNonCopyable {
private:
    T* Start;
    T* Finish;
    T* EndOfStorage;

    void StateCheck() {
        Y_ASSERT(Start <= Finish);
        Y_ASSERT(Finish <= EndOfStorage);
    }

public:
    typedef T* iterator;
    typedef const T* const_iterator;

    typedef std::reverse_iterator<iterator> reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

    TVectorSwaps()
        : Start()
        , Finish()
        , EndOfStorage()
    {
    }

    ~TVectorSwaps() {
        for (size_t i = 0; i < size(); ++i) {
            Start[i].~T();
        }
        free(Start);
    }

    operator TArrayRef<const T>() const {
        return MakeArrayRef(data(), size());
    }

    operator TArrayRef<T>() {
        return MakeArrayRef(data(), size());
    }

    size_t capacity() const {
        return EndOfStorage - Start;
    }

    size_t size() const {
        return Finish - Start;
    }

    bool empty() const {
        return size() == 0;
    }

    T* data() {
        return Start;
    }

    const T* data() const {
        return Start;
    }

    T& operator[](size_t index) {
        Y_ASSERT(index < size());
        return Start[index];
    }

    const T& operator[](size_t index) const {
        Y_ASSERT(index < size());
        return Start[index];
    }

    iterator begin() {
        return Start;
    }

    iterator end() {
        return Finish;
    }

    const_iterator begin() const {
        return Start;
    }

    const_iterator end() const {
        return Finish;
    }

    reverse_iterator rbegin() {
        return reverse_iterator(end());
    }
    reverse_iterator rend() {
        return reverse_iterator(begin());
    }

    const_reverse_iterator rbegin() const {
        return reverse_iterator(end());
    }
    const_reverse_iterator rend() const {
        return reverse_iterator(begin());
    }

    void swap(TVectorSwaps<T>& that) {
        DoSwap(Start, that.Start);
        DoSwap(Finish, that.Finish);
        DoSwap(EndOfStorage, that.EndOfStorage);
    }

    void reserve(size_t n) {
        if (n <= capacity()) {
            return;
        }

        size_t newCapacity = FastClp2(n);
        TVectorSwaps<T> tmp;
        tmp.Start = (T*)malloc(sizeof(T) * newCapacity);
        Y_ABORT_UNLESS(!!tmp.Start);

        tmp.EndOfStorage = tmp.Start + newCapacity;

        for (size_t i = 0; i < size(); ++i) {
            // TODO: catch exceptions
            new (tmp.Start + i) T();
            DoSwap(Start[i], tmp.Start[i]);
        }

        tmp.Finish = tmp.Start + size();

        swap(tmp);

        StateCheck();
    }

    void clear() {
        TVectorSwaps<T> tmp;
        swap(tmp);
    }

    template <class TIterator>
    void insert(iterator pos, TIterator b, TIterator e) {
        Y_ABORT_UNLESS(pos == end(), "TODO: only insert at the end is implemented");

        size_t count = e - b;

        reserve(size() + count);

        TIterator next = b;

        for (size_t i = 0; i < count; ++i) {
            new (Start + size() + i) T();
            DoSwap(Start[size() + i], *next);
            ++next;
        }

        Finish += count;

        StateCheck();
    }

    void push_back(T& elem) {
        insert(end(), &elem, &elem + 1);
    }
};
