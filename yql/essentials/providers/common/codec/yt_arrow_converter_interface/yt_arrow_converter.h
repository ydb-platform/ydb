#pragma once

#include <arrow/array.h>

namespace NYql {

class IYtColumnConverter {
public:
    virtual arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) = 0;
    virtual ~IYtColumnConverter() = default;
};

}
