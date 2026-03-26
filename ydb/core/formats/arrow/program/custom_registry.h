#pragma once

namespace arrow20::compute {
    class FunctionRegistry;
    class ExecContext;
}

namespace NKikimr::NArrow::NSSA {
    arrow20::compute::FunctionRegistry* GetCustomFunctionRegistry();
    arrow20::compute::ExecContext* GetCustomExecContext();
}
