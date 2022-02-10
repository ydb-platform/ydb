#pragma once

#include "defs.h"

#include <util/generic/iterator_range.h>

template<typename TDerived, typename TValue, typename TReference = TValue&, typename TDifference = ptrdiff_t>
struct TIteratorFacade {
    TReference operator *() const {
        return static_cast<const TDerived&>(*this).Dereference();
    }

    TValue *operator ->() const {
        return &**this;
    }

    TDerived& operator ++() {
        TDerived& der = static_cast<TDerived&>(*this);
        der.MoveNext();
        return der;
    }

    TDerived operator++(int) {
        TDerived& der = static_cast<TDerived&>(*this);
        TDerived ret(der);
        der.MoveNext();
        return ret;
    }

    TDerived& operator --() {
        TDerived& der = static_cast<TDerived&>(*this);
        der.MovePrev();
        return der;
    }

    TDerived operator --(int) {
        TDerived& der = static_cast<TDerived&>(*this);
        TDerived ret(der);
        der.MovePrev();
        return ret;
    }

    TDifference operator -(const TIteratorFacade& other) const {
        return static_cast<const TDerived&>(other).DistanceTo(static_cast<const TDerived&>(*this));
    }

    template<typename T>
    bool operator ==(const T& other) const {
        const TDerived& der = static_cast<const TDerived&>(*this);
        return der.EqualTo(other);
    }

    template<typename T>
    bool operator !=(const T& other) const {
        return !operator ==(other);
    }
};

