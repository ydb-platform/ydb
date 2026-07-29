#include "kernels.h"

#include <contrib/restricted/cityhash-1.0.2/city.h>

struct TCityOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char*>(p), len);
    }
};

static ui64 RunCity64(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TCityOp>(d, ws, bs, p, ctx);
}

static void* CreateCityCtx() {
    return new TCityOp();
}

static void DestroyCityCtx(void* c) {
    delete static_cast<TCityOp*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelCity64{"city64", nullptr, CreateCityCtx, DestroyCityCtx, RunCity64};
