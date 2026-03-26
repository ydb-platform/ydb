#pragma once

#include <arrow/array.h>

namespace NYql {

// Interface for converters from YT Arrow data representation into the one used by YQL computation layer
class IYtColumnConverter {
public:
    virtual arrow20::Datum Convert(std::shared_ptr<arrow20::ArrayData> block) = 0;
    virtual ~IYtColumnConverter() = default;
};

} // namespace NYql
