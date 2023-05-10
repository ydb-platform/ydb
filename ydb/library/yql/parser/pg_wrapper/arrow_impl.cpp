#include "arrow.h"

namespace NYql {

extern "C" TPgKernelState& GetPGKernelState(arrow::compute::KernelContext* ctx) {
    return dynamic_cast<TPgKernelState&>(*ctx->state());
}

extern "C" void WithPgTry(const TString& funcName, const std::function<void()>& func) {
    PG_TRY();
    {
        func();
    }
    PG_CATCH();
    {
        auto error_data = CopyErrorData();
        TStringBuilder errMsg;
        errMsg << "Error in function: " << funcName << ", reason: " << error_data->message;
        FreeErrorData(error_data);
        FlushErrorState();
        UdfTerminate(errMsg.c_str());
    }
    PG_END_TRY();
}

}
