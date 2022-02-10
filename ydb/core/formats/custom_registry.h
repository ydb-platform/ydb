#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h> 

namespace cp = ::arrow::compute;

namespace NKikimr::NArrow {
    cp::FunctionRegistry* GetCustomFunctionRegistry();
    cp::ExecContext* GetCustomExecContext();
}
