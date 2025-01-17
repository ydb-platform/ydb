#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

template<typename T>
class TIntrusiveVector : public TVector<T>, public TThrRefBase {
public:
    using TVector<T>::TVector;

    using TPtr = TIntrusivePtr<TIntrusiveVector<T>>;
    using TConstPtr = TIntrusiveConstPtr<TIntrusiveVector<T>>;

    TIntrusiveVector(const TVector<T>& other) : TVector<T>(other) {}
    TIntrusiveVector(TVector<T>&& other) : TVector<T>(std::move(other)) {}
};
