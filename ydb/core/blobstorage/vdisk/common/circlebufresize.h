#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

//////////////////////////////////////////////////////////////////////////////////////////////////////
// TAllocFreeQueue is similar to std::deque, but does not call memory alloc on push/pop in case of
// sustained load. Data is stored in circle buffer, which is reallocated on demand.
//////////////////////////////////////////////////////////////////////////////////////////////////////
template <class T>
class TAllocFreeQueue {
public:
    using TVec = TVector<T>;

    struct TIterator {
        TIterator(const TAllocFreeQueue &q)
            : Vec(q.Vec)
            , First(q.First)
            , Last(q.Last)
            , Pos(q.First)
        {}

        void SeekToFirst() {
            Pos = First;
        }

        bool Valid() const {
            return Pos != Last;
        }

        void Next() {
            ++Pos;
            if (Pos == Vec.capacity())
                Pos = 0;
        }

        const T &Get() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Vec[Pos];
        }

    private:
        const TVec &Vec;
        size_t First;
        size_t Last;
        size_t Pos;
    };

    TAllocFreeQueue(size_t capacity) {
        Vec.reserve(capacity);
    }

    size_t Size() const {
        return Elems;
    }

    void Push(const T &val) {
        if (Vec.size() < Vec.capacity()) {
            Vec.push_back(val);
            if (++Last == Vec.capacity())
                Last = 0;
        } else {
            if (Elems == Vec.capacity()) {
                // resize vector: grow its capacity
                ResizeVector();
                Push(val);
                return;
            } else {
                Vec[Last] = val;
                if (++Last == Vec.capacity())
                    Last = 0;
            }
        }
        ++Elems;
    }

    void Pop() {
        Y_DEBUG_ABORT_UNLESS(!Empty());
        --Elems;
        if (++First == Vec.capacity())
            First = 0;
    }

    bool Empty() const {
        return Elems == 0;
    }

    size_t Capacity() const {
        return Vec.capacity();
    }

    T &Top() {
        Y_DEBUG_ABORT_UNLESS(!Empty());
        return Vec[First];
    }

    const T &Top() const {
        Y_DEBUG_ABORT_UNLESS(!Empty());
        return Vec[First];
    }

    T &Back() {
        Y_DEBUG_ABORT_UNLESS(!Empty());
        return Vec[Last ? Last - 1 : Vec.size() - 1];
    }

    const T &Back() const {
        Y_DEBUG_ABORT_UNLESS(!Empty());
        return Vec[Last ? Last - 1 : Vec.size() - 1];
    }

private:
    TVec Vec;
    size_t First = 0;
    size_t Last = 0;
    size_t Elems = 0;

    void ResizeVector() {
        Y_DEBUG_ABORT_UNLESS(Elems == Vec.capacity() && Elems == Vec.size() && First == Last);
        // resize vector
        TVec newVec;
        newVec.reserve(Vec.capacity() * 2);
        for (size_t i = First; i < Vec.size(); ++i) {
            newVec.push_back(Vec[i]);
        }
        for (size_t i = 0; i < Last; ++i) {
            newVec.push_back(Vec[i]);
        }
        Vec.swap(newVec);
        First = 0;
        Last = Vec.size();
        Y_DEBUG_ABORT_UNLESS(Elems == Vec.size());
    }
};

