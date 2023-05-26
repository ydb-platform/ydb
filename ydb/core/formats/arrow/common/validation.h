#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

class TStatusValidator {
public:
    static void Validate(const arrow::Status& status) {
        Y_VERIFY(status.ok(), "%s", status.ToString().c_str());
    }

    template <class T>
    static T GetValid(const arrow::Result<T>& result) {
        Y_VERIFY(result.ok(), "%s", result.status().ToString().c_str());
        return *result;
    }

    template <class T>
    static T GetValid(arrow::Result<T>&& result) {
        Y_VERIFY(result.ok(), "%s", result.status().ToString().c_str());
        return std::move(*result);
    }
};

}
