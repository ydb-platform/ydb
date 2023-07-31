#pragma once

#include "header.h"
#include "extinfo.h"

#include <util/generic/vector.h>

#include <errno.h>

///////////////////////////////////////////////////////////////////////////////

/// Default comparator
template <class TVal>
struct TCompareByLess {
    inline bool operator()(const TVal* a, const TVal* b) const {
        return TLess<TVal>()(*a, *b);
    }
};

///////////////////////////////////////////////////////////////////////////////

template <class TVal, class TIterator, class TCompare = TCompareByLess<TVal>>
class THeapIter {
public:
    int Init(TIterator** iters, int count) {
        Term();
        if (!count)
            return 0;
        if (!(Heap = (TIterator**)malloc(count * sizeof(TIterator*))))
            return ENOMEM;

        Count = count;
        count = 0;
        while (count < Count)
            if (count && !(*iters)->Next()) { //here first TIterator is NOT initialized!
                Count--;
                iters++;
            } else {
                Heap[count++] = *iters++;
            }
        count = Count / 2;
        while (--count > 0)     //Heap[0] is not changed!
            Sift(count, Count); //do not try to replace this code by make_heap
        return 0;
    }

    int Init(TIterator* iters, int count) {
        TVector<TIterator*> a(count);
        for (int i = 0; i < count; ++i)
            a[i] = &iters[i];
        return Init(&a[0], count);
    }

    THeapIter()
        : Heap(nullptr)
        , Count(0)
    {
    }

    THeapIter(TIterator* a, TIterator* b)
        : Heap(nullptr)
        , Count(0)
    {
        TIterator* arr[] = {a, b};
        if (Init(arr, 2))
            ythrow yexception() << "can't Init THeapIter";
    }

    THeapIter(TVector<TIterator>& v)
        : Heap(nullptr)
        , Count(0)
    {
        if (Init(&v[0], v.size())) {
            ythrow yexception() << "can't Init THeapIter";
        }
    }

    ~THeapIter() {
        Term();
    }

    inline const TVal* Current() const {
        if (!Count)
            return nullptr;
        return (*Heap)->Current();
    }

    inline const TIterator* CurrentIter() const {
        return *Heap;
    }

    //for ends of last file will use Heap[0] = Heap[0] ! and
    //returns Current of eof so Current of eof MUST return NULL
    //possible this is bug and need fixing
    const TVal* Next() {
        if (!Count)
            return nullptr;
        if (!(*Heap)->Next())      //on first call unitialized first TIterator
            *Heap = Heap[--Count]; //will be correctly initialized

        if (Count == 2) {
            if (TCompare()(Heap[1]->Current(), Heap[0]->Current()))
                DoSwap(Heap[1], Heap[0]);
        } else
            Sift(0, Count);

        return Current();
    }

    inline bool GetExtInfo(typename TExtInfoType<TVal>::TResult* extInfo) const {
        return (*Heap)->GetExtInfo(extInfo);
    }

    inline const ui8* GetExtInfoRaw(size_t* len) const {
        return (*Heap)->GetExtInfoRaw(len);
    }

    void Term() {
        ::free(Heap);
        Heap = nullptr;
        Count = 0;
    }

protected:
    void Sift(int node, int end) {
        TIterator* x = Heap[node];
        int son;
        for (son = 2 * node + 1; son < end; node = son, son = 2 * node + 1) {
            if (son < (end - 1) && TCompare()(Heap[son + 1]->Current(), Heap[son]->Current()))
                son++;
            if (TCompare()(Heap[son]->Current(), x->Current()))
                Heap[node] = Heap[son];
            else
                break;
        }
        Heap[node] = x;
    }

    TIterator** Heap;
    int Count;
};

///////////////////////////////////////////////////////////////////////////////
