#include "erasure_isa.h"

#include <util/system/yassert.h>

#include <algorithm>
#include <limits>

extern "C" {
#include <contrib/libs/isa-l/include/erasure_code.h>
}

namespace NKikimr {
namespace {

bool Overlap(const ui8* a, const ui8* b, size_t len) {
    const auto left = reinterpret_cast<uintptr_t>(a);
    const auto right = reinterpret_cast<uintptr_t>(b);
    return left < right ? right - left < len : left - right < len;
}

void Run(size_t len, ui32 rows, const ui8* tables, ui8* sources[TErasureIsaL::DataPartCount + 1],
        ui8* outputs[TErasureIsaL::ParityPartCount]) {
    Y_ABORT_UNLESS(len <= static_cast<size_t>(std::numeric_limits<int>::max()));
    for (ui32 i = 0; i < TErasureIsaL::DataPartCount; ++i) {
        Y_ABORT_UNLESS(sources[i]);
    }
    for (ui32 i = 0; i < rows; ++i) {
        Y_ABORT_UNLESS(outputs[i]);
        for (ui32 j = 0; j < TErasureIsaL::DataPartCount; ++j) {
            Y_ABORT_UNLESS(!Overlap(outputs[i], sources[j], len));
        }
        for (ui32 j = 0; j < i; ++j) {
            Y_ABORT_UNLESS(!Overlap(outputs[i], outputs[j], len));
        }
    }

    // ISA-L 2.31 SVE multi-output kernels fetch one pointer after the last
    // source. They do not dereference it, but the extra slot must exist.
    sources[TErasureIsaL::DataPartCount] = sources[TErasureIsaL::DataPartCount - 1];
    ec_encode_data(static_cast<int>(len), TErasureIsaL::DataPartCount, rows,
        const_cast<ui8*>(tables), sources, outputs);
}

} // anonymous namespace

TErasureIsaL::TErasureIsaL() {
    BasisByMissingMask.fill(InvalidBasis);
    gf_gen_rs_matrix(Matrix.data(), TotalPartCount, DataPartCount);
    ec_init_tables(DataPartCount, ParityPartCount, Matrix.data() + DataPartCount * DataPartCount,
        EncodeTables.data());

    ui32 index = 0;
    for (ui32 a = 0; a < TotalPartCount; ++a) {
        for (ui32 b = a + 1; b < TotalPartCount; ++b) {
            const ui16 excludedMask = (1u << a) | (1u << b);
            BasisByMissingMask[excludedMask] = index;
            auto& basis = Bases[index++];
            basis.Targets = {static_cast<ui8>(a), static_cast<ui8>(b)};

            std::array<ui8, DataPartCount * DataPartCount> sourceMatrix{};
            std::array<ui8, DataPartCount * DataPartCount> inverse{};
            ui32 row = 0;
            for (ui32 part = 0; part < TotalPartCount; ++part) {
                if (!(excludedMask & (1u << part))) {
                    basis.Sources[row] = part;
                    std::copy_n(Matrix.data() + part * DataPartCount, DataPartCount,
                        sourceMatrix.data() + row++ * DataPartCount);
                }
            }
            Y_ABORT_UNLESS(row == DataPartCount);
            Y_ABORT_UNLESS(gf_invert_matrix(sourceMatrix.data(), inverse.data(), DataPartCount) == 0);
            for (ui32 target = 0; target < ParityPartCount; ++target) {
                for (ui32 column = 0; column < DataPartCount; ++column) {
                    ui8 coefficient = 0;
                    for (ui32 j = 0; j < DataPartCount; ++j) {
                        coefficient ^= gf_mul_erasure(Matrix[basis.Targets[target] * DataPartCount + j],
                            inverse[j * DataPartCount + column]);
                    }
                    basis.Coefficients[target * DataPartCount + column] = coefficient;
                }
            }
            ec_init_tables(DataPartCount, ParityPartCount, basis.Coefficients.data(), basis.TablesAB.data());
            ec_init_tables(DataPartCount, 1, basis.Coefficients.data() + DataPartCount, basis.TablesB.data());
            basis.Plans[0] = {{static_cast<ui8>(a), 0}, 1, false};
            basis.Plans[1] = {{static_cast<ui8>(b), 0}, 1, true};
            basis.Plans[2] = {{static_cast<ui8>(a), static_cast<ui8>(b)}, 2, false};
        }
    }
    Y_ABORT_UNLESS(index == BasisCount);

    // Zero/single losses reuse the basis formed by the first eight survivors.
    // In particular, a single loss never forces reading the ninth survivor.
    for (ui32 lost = 0; lost <= TotalPartCount; ++lost) {
        const ui16 missingMask = lost < TotalPartCount ? 1u << lost : 0;
        ui16 selectedMask = 0;
        ui32 count = 0;
        for (ui32 part = 0; part < TotalPartCount && count < DataPartCount; ++part) {
            if (!(missingMask & (1u << part))) {
                selectedMask |= 1u << part;
                ++count;
            }
        }
        BasisByMissingMask[missingMask] = BasisByMissingMask[FullMask ^ selectedMask];
        Y_ABORT_UNLESS(BasisByMissingMask[missingMask] != InvalidBasis);
    }

    // ec_init_tables and ec_encode_data have separate lazy dispatchers.
    alignas(64) std::array<std::array<ui8, 32>, TotalPartCount> buffers{};
    std::array<const ui8*, DataPartCount> sources{};
    for (ui32 i = 0; i < DataPartCount; ++i) {
        sources[i] = buffers[i].data();
    }
    std::array<ui8*, ParityPartCount> outputs{buffers[DataPartCount].data(), buffers[DataPartCount + 1].data()};
    Encode(buffers[0].size(), sources.data(), outputs.data());
}

const TErasureIsaL::TBasis& TErasureIsaL::Basis(ui16 missingMask) const {
    Y_ABORT_UNLESS(!(missingMask & ~FullMask));
    const ui8 index = BasisByMissingMask[missingMask];
    Y_ABORT_UNLESS(index != InvalidBasis, "block-8-2 requires at least eight available parts");
    return Bases[index];
}

const std::array<ui8, TErasureIsaL::DataPartCount>& TErasureIsaL::Sources(ui16 missingMask) const {
    return Basis(missingMask).Sources;
}

void TErasureIsaL::Encode(size_t len, const ui8* const input[DataPartCount],
        ui8* const output[ParityPartCount]) const {
    if (!len) {
        return;
    }
    std::array<ui8*, DataPartCount + 1> sources{};
    for (ui32 i = 0; i < DataPartCount; ++i) {
        sources[i] = const_cast<ui8*>(input[i]);
    }
    std::array<ui8*, ParityPartCount> outputs{output[0], output[1]};
    Run(len, ParityPartCount, EncodeTables.data(), sources.data(), outputs.data());
}

void TErasureIsaL::Restore(size_t len, ui16 missingMask, ui16 neededOutputMask,
        ui8* const slots[TotalPartCount]) const {
    const auto& basis = Basis(missingMask);
    Y_ABORT_UNLESS(!(neededOutputMask & ~missingMask));
    if (!len || !neededOutputMask) {
        return;
    }
    const ui16 first = 1u << basis.Targets[0];
    const ui16 second = 1u << basis.Targets[1];
    const auto& plan = basis.Plans[neededOutputMask == first ? 0 : neededOutputMask == second ? 1 : 2];
    std::array<ui8*, DataPartCount + 1> sources{};
    for (ui32 i = 0; i < DataPartCount; ++i) {
        sources[i] = slots[basis.Sources[i]];
    }
    std::array<ui8*, ParityPartCount> outputs{};
    for (ui32 i = 0; i < plan.OutputCount; ++i) {
        outputs[i] = slots[plan.Targets[i]];
    }
    Run(len, plan.OutputCount, plan.SecondTable ? basis.TablesB.data() : basis.TablesAB.data(),
        sources.data(), outputs.data());
}

const TErasureIsaL& GetErasureIsaL() {
    static const TErasureIsaL backend;
    return backend;
}

static_assert(sizeof(TErasureIsaL) < 40 * 1024);

} // namespace NKikimr
