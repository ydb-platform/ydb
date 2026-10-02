#pragma once

namespace arrow::compute {
    class FunctionRegistry;
    class ExecContext;
}

namespace NKikimr::NArrow::NSSA {
    arrow::compute::FunctionRegistry* GetCustomFunctionRegistry();
    arrow::compute::ExecContext* GetCustomExecContext();
}
