#pragma once

#include "defs.h"

template <typename TDerived>
struct TNamedTupleBase {
    friend bool operator==(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() == y.ConvertToTuple();
    }

    friend bool operator!=(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() != y.ConvertToTuple();
    }

    friend bool operator<(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() < y.ConvertToTuple();
    }

    friend bool operator<=(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() <= y.ConvertToTuple();
    }

    friend bool operator>(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() > y.ConvertToTuple();
    }

    friend bool operator>=(const TDerived& x, const TDerived& y) {
        return x.ConvertToTuple() >= y.ConvertToTuple();
    }
};
