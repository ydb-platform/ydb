#pragma once

#include <library/cpp/threading/future/future.h>

namespace NYql {
/*
Let's call 'action' attached functor to future which should be called in the main thread.
This trick allows us to execute attached actions not in the future's execution thread but rather in the main thread
and preserve single-threaded logic.
*/

/*
Make ready future with constant action inside.
*/
template <typename T>
NThreading::TFuture<std::function<T()>> MakeFutureWithConstantAction(const T& value) {
    return NThreading::MakeFuture<std::function<T()>>([value]() {
        return value;
    });
}

template <typename T, typename A>
auto AddActionToFuture(NThreading::TFuture<T> f, const A& action) {
    using V = decltype(action(std::declval<T>()));

    return f.Apply([action](NThreading::TFuture<T> f) {
        std::function<V()> r = [f, action]() {
            return action(f.GetValue());
        };

        return r;
    });
}

/*
Apply continuation with constant action
*/
template <typename T, typename V>
NThreading::TFuture<std::function<V()>> AddConstantActionToFuture(NThreading::TFuture<T> f, const V& value) {
    return AddActionToFuture(f, [value](const T&) { return value; });
}

/*
Transform action result by applying mapper
*/
template <typename R, typename TMapper, typename ...Args>
auto MapFutureAction(NThreading::TFuture<std::function<R(Args&&...)>> f, const TMapper& mapper) {
    using V = decltype(mapper(std::declval<R>()));

    return f.Apply([mapper](NThreading::TFuture<std::function<R(Args&&...)>> f) {
        std::function<V(Args&&...)> r = [f, mapper](Args&& ...args) {
            return mapper(f.GetValue()(std::forward<Args>(args)...));
        };

        return r;
    });
}

}
