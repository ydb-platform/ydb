#pragma once

namespace arrow::compute {
    class FunctionRegistry;
    class ExecContext;
}

namespace NKikimr::NArrow {
    arrow::compute::FunctionRegistry* GetCustomFunctionRegistry();
    arrow::compute::ExecContext* GetCustomExecContext();
}
