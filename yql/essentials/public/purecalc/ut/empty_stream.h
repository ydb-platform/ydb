#pragma once

#include <yql/essentials/public/purecalc/purecalc.h>

namespace NYql::NPureCalc {
template <typename T>
class TEmptyStreamImpl: public IStream<T> {
public:
    T Fetch() override {
        return nullptr;
    }
};

template <typename T>
THolder<IStream<T>> EmptyStream() {
    return MakeHolder<TEmptyStreamImpl<T>>();
}
} // namespace NYql::NPureCalc
