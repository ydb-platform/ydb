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
        : Factory_(std::move(factory))
    {
    }

    T* Get() const noexcept {
        if (!Value_) {
            Value_ = Factory_();
        }
        return Value_.Get();
    }

    inline explicit operator bool() const noexcept {
        return !!Value_.Get();
    }

private:
    TFactory Factory_;
    mutable THolder<T> Value_;

};

} // NYql
