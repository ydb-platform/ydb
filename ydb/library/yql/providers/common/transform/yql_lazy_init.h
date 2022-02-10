#pragma once

#include <util/generic/ptr.h>

#include <functional>
#include <utility>

namespace NYql {

template <class T>
class TLazyInitHolder
    : public TPointerBase<TLazyInitHolder<T>, T>
{
public:
    using TFactory = std::function<THolder<T>()>;

    TLazyInitHolder(TFactory&& factory)
        : Factory(std::move(factory))
    {
    }

    T* Get() const noexcept {
        if (!Value) {
            Value = Factory();
        }
        return Value.Get();
    }

private:
    TFactory Factory;
    mutable THolder<T> Value;

};

} // NYql
