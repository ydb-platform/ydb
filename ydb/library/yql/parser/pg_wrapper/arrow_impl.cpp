#include "arrow.h"

namespace NYql {

extern "C" TPgKernelState& GetPGKernelState(arrow::compute::KernelContext* ctx) {
    return dynamic_cast<TPgKernelState&>(*ctx->state());
}

}
