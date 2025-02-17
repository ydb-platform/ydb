#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

class TStatusValidator {
public:
    static void Validate(const arrow::Status& status);

    template <class T>
    static T GetValid(const arrow::Result<T>& result) {
        Validate(result.status());
        return *result;
    }

    template <class T>
    static T GetValid(arrow::Result<T>&& result) {
        Validate(result.status());
        return std::move(*result);
    }
};

}
