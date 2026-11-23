#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>

namespace NKikimr::NKernels {

arrow::compute::ExecContext* GetCustomExecContext();

}
