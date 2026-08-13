#pragma once

#include <library/cpp/threading/future/core/future.h>

namespace {

template <typename T>
T ExtractFromConstFuture(const NThreading::TFuture<T>& f) {
    // We want to avoid making a copy of data stored in a future.
    // But there is no direct way to extract data from a const future
    // So, we make a copy of the future, that is cheap. Then, extract the value from this copy.
    // It destructs the value in the original future, but this trick is legal and documented here:
    // https://docs.yandex-team.ru/arcadia-cpp/cookbook/concurrency
    return NThreading::TFuture<T>(f).ExtractValueSync();
}

} // namespace
