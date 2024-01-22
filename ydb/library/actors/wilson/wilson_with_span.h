#pragma once

#include "wilson_span.h"

namespace NWilson {

    template<class T>
    struct TWithSpan {
        TWithSpan(T&& item, NWilson::TSpan&& span)
            : Item(std::forward<T>(item))
            , Span(std::move(span))
        {}

        T Item;
        NWilson::TSpan Span;
    };

} // NWilson
