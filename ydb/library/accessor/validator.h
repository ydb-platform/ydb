#pragma once

#include <ydb/library/actors/core/log.h>

class TValidator {
public:
    template <class T>
    static const T& CheckNotNull(const T& object) {
        AFL_VERIFY(!!object);
        return object;
    }
};