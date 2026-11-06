#pragma once

#include <ydb/library/actors/core/log.h>

class TValidator {
public:
    template <class T>
    static const T& CheckNotNull(const T& object) {
        YDB_LOG_VERIFY(!!object);
        return object;
    }
    template <class T>
    static T&& CheckNotNull(T&& object) {
        YDB_LOG_VERIFY(!!object);
        return std::forward<T>(object);
    }
};
