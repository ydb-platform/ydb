#pragma once
#include "yql_result_format_common.h"

namespace NYql::NResult {

inline void Check(bool value, const TSourceLocation& location) {
    if (!value) {
        throw location + TUnsupportedException();
    }
}

#define CHECK(value) Check(value, __LOCATION__)
#define UNEXPECTED ythrow TUnsupportedException() << "Unhandled case"

}
