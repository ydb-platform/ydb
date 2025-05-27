#pragma once

#include <arrow/array.h>

namespace NYql {

// Interface for converters from YT Arrow data representation into the one used by YQL computation layer
class IYtColumnConverter {
public:
    virtual arrow::Datum Convert(std::shared_ptr<arrow::ArrayData> block) = 0;
    virtual ~IYtColumnConverter() = default;
};

}
