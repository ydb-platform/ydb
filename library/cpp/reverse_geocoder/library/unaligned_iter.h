#pragma once

#include <util/system/unaligned_mem.h>
#include <iterator>

namespace NReverseGeocoder {
    /**
     * Random-access iterator over a read-only memory range
     * of trivially copyable items that may be not aligned properly.
     *
     * When dereferencing, a copy of item is returned, not a reference.
     * Be sure that sizeof(T) is small enough.
     *
     * Iterator is useful for LowerBound/UpperBound STL algorithms.
     */
    template <class T>
    class TUnalignedIter: public std::iterator<std::random_access_iterator_tag, T> {
    public:
        using TSelf = TUnalignedIter<T>;

        explicit TUnalignedIter(const T* ptr)
            : Ptr(ptr)
        {
        }

        T operator*() const {
            return ReadUnaligned<T>(Ptr);
        }

        bool operator==(TSelf other) const {
            return Ptr == other.Ptr;
        }

        bool operator<(TSelf other) const {
            return Ptr < other.Ptr;
        }

        TSelf operator+(ptrdiff_t delta) const {
            return TSelf{Ptr + delta};
        }

        ptrdiff_t operator-(TSelf other) const {
            return Ptr - other.Ptr;
        }

        TSelf& operator+=(ptrdiff_t delta) {
            Ptr += delta;
            return *this;
        }

        TSelf& operator++() {
            ++Ptr;
            return *this;
        }

    private:
        const T* Ptr;
    };

    template <class T>
    TUnalignedIter<T> UnalignedIter(const T* ptr) {
        return TUnalignedIter<T>(ptr);
    }
}
