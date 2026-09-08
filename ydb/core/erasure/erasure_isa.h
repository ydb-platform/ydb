#pragma once

#include <util/system/types.h>

#include <array>

namespace NKikimr {

// Private block-8-2 kernel API. The caller owns equal-sized source/output spans,
// their lifetime, layout, padding and CRC. Sources are never modified.
class TErasureIsaL {
public:
    static constexpr ui32 DataPartCount = 8;
    static constexpr ui32 ParityPartCount = 2;
    static constexpr ui32 TotalPartCount = DataPartCount + ParityPartCount;
    static constexpr ui16 FullMask = (1u << TotalPartCount) - 1;
    static constexpr ui32 BasisCount = 45;
    static constexpr ui32 PlanCount = BasisCount * 3;

    void Encode(size_t len, const ui8* const sources[DataPartCount],
        ui8* const outputs[ParityPartCount]) const;

    // Only neededOutputMask slots are written; it must be a subset of missingMask.
    // Unrequested missing slots may be null. A zero length/output mask is a no-op.
    void Restore(size_t len, ui16 missingMask, ui16 neededOutputMask,
        ui8* const slots[TotalPartCount]) const;

    // The first eight available physical slots, in ascending (data-first) order.
    const std::array<ui8, DataPartCount>& Sources(ui16 missingMask) const;

private:
    friend const TErasureIsaL& GetErasureIsaL();

    static constexpr ui8 InvalidBasis = 0xff;
    static constexpr ui32 TableRowSize = 32 * DataPartCount;

    struct TPlan {
        std::array<ui8, ParityPartCount> Targets{};
        ui8 OutputCount = 0;
        bool SecondTable = false;
    };

    struct TBasis {
        // ISA-L dispatch may select a platform-specific expanded-table format.
        // A uses the prefix of AB; B has its own one-output table.
        alignas(64) std::array<ui8, TableRowSize * ParityPartCount> TablesAB{};
        alignas(64) std::array<ui8, TableRowSize> TablesB{};
        std::array<ui8, DataPartCount> Sources{};
        std::array<ui8, ParityPartCount> Targets{};
        std::array<ui8, DataPartCount * ParityPartCount> Coefficients{};
        std::array<TPlan, 3> Plans{};
    };

    TErasureIsaL();
    TErasureIsaL(const TErasureIsaL&) = delete;
    TErasureIsaL& operator=(const TErasureIsaL&) = delete;

    const TBasis& Basis(ui16 missingMask) const;

    std::array<ui8, TotalPartCount * DataPartCount> Matrix{};
    alignas(64) std::array<ui8, TableRowSize * ParityPartCount> EncodeTables{};
    std::array<TBasis, BasisCount> Bases{};
    std::array<ui8, 1u << TotalPartCount> BasisByMissingMask{};
};

// Fully constructs all 45 bases / 135 plans and warms the encode dispatcher
// before publishing the immutable singleton to concurrent callers.
const TErasureIsaL& GetErasureIsaL();

} // namespace NKikimr
