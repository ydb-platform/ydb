#pragma once

#include <util/generic/vector.h>
#include <util/system/yassert.h>

template <class T>
class TCircleBuf {
public:
    typedef TVector<T> TVec;

    struct TIterator {
        TIterator(TVec &vec, size_t pos)
            : Vec(vec)
            , Pos(pos)
        {}

        T *operator ->() {
            return &Vec[Pos % Vec.size()];
        }

        T &operator *() {
            return Vec[Pos % Vec.size()];
        }

        TIterator &operator ++() {
            ++Pos;
            return *this;
        }

        TIterator operator ++(int) {
            TIterator tmp(Vec, Pos);
            ++Pos;
            return tmp;
        }

        bool operator ==(const TIterator &i) {
            Y_DEBUG_ABORT_UNLESS(Vec == i.Vec);
            return Pos == i.Pos;
        }

        bool operator !=(const TIterator &i) {
            return !operator == (i);
        }

    private:
        TVec &Vec;
        size_t Pos;
    };


    TCircleBuf(size_t size) {
        Vec.reserve(size);
        Size = size;
        Pos = 0;
    }

    void Push(const T &val) {
        if (Vec.size() < Size) {
            Vec.push_back(val);
        } else {
            Vec[Pos++] = val;
            Pos %= Size;
        }
    }

    TIterator Begin() {
        return TIterator(Vec, Pos);
    }

    TIterator End() {
        return TIterator(Vec, Pos + Vec.size());
    }

    const T& First() const {
        return Vec[Pos];
    }

    const T& Last() const {
        return Vec[(Pos + Vec.size() - 1) % Vec.size()];
    }

    operator bool() const {
        return !Vec.empty();
    }

private:
    TVec Vec;
    size_t Size;
    size_t Pos;
};

