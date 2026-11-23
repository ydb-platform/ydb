#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NKernels {

std::shared_ptr<arrow::Array> NumVecToArray(
    const std::shared_ptr<arrow::DataType>& type, const std::vector<double>& vec, std::optional<double> nullValue = {});

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<std::optional<bool>>& vec);
std::shared_ptr<arrow::Array> StringVecToArray(const std::vector<std::optional<std::string>>& vec);
std::shared_ptr<arrow::Array> UInt8VecToArray(const std::vector<std::optional<uint8_t>>& vec);

}   // namespace NKikimr::NKernels
