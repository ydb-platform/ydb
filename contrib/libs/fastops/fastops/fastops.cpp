#include "fastops.h"

#include <fastops/avx/ops_avx.h>
#include <fastops/avx2/ops_avx2.h>
#include <fastops/plain/ops_plain.h>

#include <fastops/core/avx_id.h>

namespace NFastOps {
    template <bool I_Exact, bool I_OutAligned>
    void Exp(const float* from, size_t size, float* to) {
        if (HaveAvx2()) {
            ExpAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            ExpAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            ExpPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Exp(const double* from, size_t size, double* to) {
        if (HaveAvx2()) {
            ExpAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            ExpAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            ExpPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Log(const float* from, size_t size, float* to) {
        if (HaveAvx2()) {
            LogAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            LogAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            LogPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Log(const double* from, size_t size, double* to) {
        if (HaveAvx2()) {
            LogAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            LogAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            LogPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Sigmoid(const float* from, size_t size, float* to) {
        if (HaveAvx2()) {
            SigmoidAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            SigmoidAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            SigmoidPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Sigmoid(const double* from, size_t size, double* to) {
        if (HaveAvx2()) {
            SigmoidAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            SigmoidAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            SigmoidPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Tanh(const float* from, size_t size, float* to) {
        if (HaveAvx2()) {
            TanhAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            TanhAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            TanhPlain(from, size, to);
        }
    }

    template <bool I_Exact, bool I_OutAligned>
    void Tanh(const double* from, size_t size, double* to) {
        if (HaveAvx2()) {
            TanhAvx2<I_Exact, I_OutAligned>(from, size, to);
        } else if (HaveAvx()) {
            TanhAvx<I_Exact, I_OutAligned>(from, size, to);
        } else {
            TanhPlain(from, size, to);
        }
    }

    template void Exp<false, false>(const float* from, size_t size, float* to);
    template void Exp<false, true>(const float* from, size_t size, float* to);
    template void Exp<true, false>(const float* from, size_t size, float* to);
    template void Exp<true, true>(const float* from, size_t size, float* to);

    template void Exp<false, false>(const double* from, size_t size, double* to);
    template void Exp<false, true>(const double* from, size_t size, double* to);
    template void Exp<true, false>(const double* from, size_t size, double* to);
    template void Exp<true, true>(const double* from, size_t size, double* to);

    template void Log<false, false>(const float* from, size_t size, float* to);
    template void Log<false, true>(const float* from, size_t size, float* to);
    template void Log<true, false>(const float* from, size_t size, float* to);
    template void Log<true, true>(const float* from, size_t size, float* to);

    template void Log<false, false>(const double* from, size_t size, double* to);
    template void Log<false, true>(const double* from, size_t size, double* to);
    template void Log<true, false>(const double* from, size_t size, double* to);
    template void Log<true, true>(const double* from, size_t size, double* to);

    template void Sigmoid<false, false>(const float* from, size_t size, float* to);
    template void Sigmoid<false, true>(const float* from, size_t size, float* to);
    template void Sigmoid<true, false>(const float* from, size_t size, float* to);
    template void Sigmoid<true, true>(const float* from, size_t size, float* to);

    template void Sigmoid<false, false>(const double* from, size_t size, double* to);
    template void Sigmoid<false, true>(const double* from, size_t size, double* to);
    template void Sigmoid<true, false>(const double* from, size_t size, double* to);
    template void Sigmoid<true, true>(const double* from, size_t size, double* to);

    template void Tanh<false, false>(const float* from, size_t size, float* to);
    template void Tanh<false, true>(const float* from, size_t size, float* to);
    template void Tanh<true, false>(const float* from, size_t size, float* to);
    template void Tanh<true, true>(const float* from, size_t size, float* to);

    template void Tanh<false, false>(const double* from, size_t size, double* to);
    template void Tanh<false, true>(const double* from, size_t size, double* to);
    template void Tanh<true, false>(const double* from, size_t size, double* to);
    template void Tanh<true, true>(const double* from, size_t size, double* to);
}
