#include <cmath>
#include <cstdint>
#include <iterator>
#include <ctime>
#include <vector>
#include <algorithm>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/registry_internal.h>

#include <library/cpp/testing/unittest/registar.h>

#include "func_common.h"
#include "functions.h"

namespace NKikimr::NKernels {

std::shared_ptr<arrow20::Array> NumVecToArray(const std::shared_ptr<arrow20::DataType>& type,
                                            const std::vector<double>& vec,
                                            std::optional<double> nullValue = {});

arrow20::compute::ExecContext* GetCustomExecContext();

}
