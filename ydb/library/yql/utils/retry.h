#pragma once

namespace NYql {

template <typename TRetriableException, typename TAction, typename TExceptionHandler>
auto WithRetry(int attempts, TAction&& a, TExceptionHandler&& exceptionHandler) {
    for (int i = 1; i < attempts; ++i) {
        try {
            return a();
        } catch (const TRetriableException& e) {
            exceptionHandler(e, i, attempts);
        }
    }

    return a();
}
}
