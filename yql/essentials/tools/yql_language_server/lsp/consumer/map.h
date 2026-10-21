#pragma once

#include "base.h"

namespace NLsp {

namespace NDetail {

template <typename T, typename U, typename F>
class TMapConsumer final: public IConsumer<T> {
    static_assert(std::is_same_v<decltype(std::declval<F>()(std::declval<T>())), U>);

public:
    TMapConsumer(F f, IConsumer<U>::TPtr consumer)
        : F_(std::move(f))
        , Consumer_(std::move(consumer))
    {
    }

    void Receive(T value) override {
        Consumer_->Receive(F_(std::move(value)));
    }

    void Stop() override {
        Consumer_->Stop();
    }

private:
    F F_;
    IConsumer<U>::TPtr Consumer_;
};

} // namespace NDetail

template <typename T, typename U, typename F>
IConsumer<T>::TPtr Map(F f, typename IConsumer<U>::TPtr consumer) {
    return new NDetail::TMapConsumer<T, U, F>(std::move(f), std::move(consumer));
}

} // namespace NLsp
