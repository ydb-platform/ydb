#pragma once

namespace NFastOps {
    namespace NDetail {
        bool IsAVXEnabled() noexcept;
        bool IsAVX2Enabled() noexcept;
    }

    inline bool HaveAvx() {
        static bool haveAVX = NDetail::IsAVXEnabled();
        return haveAVX;
    }

    inline bool HaveAvx2() {
        static bool haveAVX2 = HaveAvx() && NDetail::IsAVX2Enabled();
        return haveAVX2;
    }
}