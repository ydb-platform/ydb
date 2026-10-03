#include <cmath>
#include <cstdint>
#include <iterator>
#include <ctime>
#include <optional>
#include <string>
#include <vector>
#include <algorithm>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry_internal.h>

#include <library/cpp/testing/unittest/registar.h>

#include "func_common.h"
#include "functions.h"

namespace NKikimr::NKernels {

std::shared_ptr<arrow::Array> NumVecToArray(const std::shared_ptr<arrow::DataType>& type,
                                            const std::vector<double>& vec,
                                            std::optional<double> nullValue = {});

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<std::optional<bool>>& vec);
std::shared_ptr<arrow::Array> StringVecToArray(const std::vector<std::optional<std::string>>& vec);
std::shared_ptr<arrow::Array> UInt8VecToArray(const std::vector<std::optional<uint8_t>>& vec);

arrow::compute::ExecContext* GetCustomExecContext();

}
