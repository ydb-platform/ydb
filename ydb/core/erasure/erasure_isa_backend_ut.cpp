#include "erasure_isa.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/platform.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

#ifdef _unix_
#include <sys/mman.h>
#include <unistd.h>
#endif

#if defined(_linux_) && defined(_arm64_)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

extern "C" {
#include <contrib/libs/isa-l/include/erasure_code.h>
#if defined(_linux_) && defined(_arm64_)
void ec_encode_data_neon(int len, int k, int rows, ui8* tables, ui8** sources, ui8** outputs);
void ec_encode_data_sve(int len, int k, int rows, ui8* tables, ui8** sources, ui8** outputs);
#endif
}

namespace NKikimr {
namespace {

// Deliberately independent of ISA-L's GF primitives and generated matrix.
ui8 Multiply(ui8 a, ui8 b) {
    ui16 left = a;
    ui8 result = 0;
    for (ui32 bit = 0; bit < 8; ++bit) {
        if (b & (1u << bit)) {
            result ^= left;
        }
        left <<= 1;
        if (left & 0x100) {
            left ^= 0x11d;
        }
    }
    return result;
}

ui8 Inverse(ui8 value) {
    ui8 result = 1;
    for (ui32 power = 254; power; power >>= 1) {
        if (power & 1) {
            result = Multiply(result, value);
        }
        value = Multiply(value, value);
    }
    return result;
}

ui8 Matrix(ui32 row, ui32 column) {
    return row < 8 ? row == column : row == 8 ? 1 : 1u << column;
}

std::array<std::array<ui8, 8>, 8> InvertBasis(const std::array<ui8, 8>& sources) {
    std::array<std::array<ui8, 16>, 8> augmented{};
    for (ui32 row = 0; row < 8; ++row) {
        for (ui32 column = 0; column < 8; ++column) {
            augmented[row][column] = Matrix(sources[row], column);
        }
        augmented[row][8 + row] = 1;
    }
    for (ui32 column = 0; column < 8; ++column) {
        ui32 pivot = column;
        while (pivot < 8 && !augmented[pivot][column]) {
            ++pivot;
        }
        UNIT_ASSERT(pivot < 8);
        std::swap(augmented[column], augmented[pivot]);
        const ui8 scale = Inverse(augmented[column][column]);
        for (auto& value : augmented[column]) {
            value = Multiply(value, scale);
        }
        for (ui32 row = 0; row < 8; ++row) {
            if (row != column) {
                const ui8 factor = augmented[row][column];
                for (ui32 j = 0; j < 16; ++j) {
                    augmented[row][j] ^= Multiply(factor, augmented[column][j]);
                }
            }
        }
    }
    std::array<std::array<ui8, 8>, 8> inverse{};
    for (ui32 row = 0; row < 8; ++row) {
        std::copy_n(augmented[row].begin() + 8, 8, inverse[row].begin());
    }
    return inverse;
}

using TBuffers = std::array<std::vector<ui8>, 10>;

TBuffers MakeBuffers(size_t len) {
    TBuffers buffers;
    for (ui32 part = 0; part < 10; ++part) {
        buffers[part].resize(len + 2, 0xa5);
        for (size_t offset = 0; offset < len; ++offset) {
            buffers[part][offset + 1] = part < 8 ? (offset * 117 + part * 73 + (offset >> 3)) & 255 : 0;
        }
    }
    for (ui32 target = 8; target < 10; ++target) {
        for (ui32 part = 0; part < 8; ++part) {
            for (size_t offset = 0; offset < len; ++offset) {
                buffers[target][offset + 1] ^= Multiply(Matrix(target, part), buffers[part][offset + 1]);
            }
        }
    }
    return buffers;
}

std::array<ui8*, 10> Slots(TBuffers& buffers) {
    std::array<ui8*, 10> slots{};
    for (ui32 part = 0; part < 10; ++part) {
        slots[part] = buffers[part].data() + 1; // Intentionally unaligned.
    }
    return slots;
}

void VerifyRestore(const TBuffers& original, ui16 missing, ui16 needed, bool onlyEightSources) {
    const auto& codec = GetErasureIsaL();
    auto buffers = original;
    auto slots = Slots(buffers);
    for (ui32 part = 0; part < 10; ++part) {
        if (missing & (1u << part)) {
            std::fill(buffers[part].begin() + 1, buffers[part].end() - 1, 0xa5);
            if (!(needed & (1u << part))) {
                slots[part] = nullptr;
            }
        }
    }
    if (onlyEightSources) {
        const auto& sources = codec.Sources(missing);
        for (ui32 part = 0; part < 10; ++part) {
            if (!(needed & (1u << part)) && std::find(sources.begin(), sources.end(), part) == sources.end()) {
                slots[part] = nullptr;
            }
        }
    }
    const auto before = buffers;
    codec.Restore(original[0].size() - 2, missing, needed, slots.data());
    for (ui32 part = 0; part < 10; ++part) {
        UNIT_ASSERT_C(buffers[part] == (needed & (1u << part) ? original[part] : before[part]),
            "missing=" << missing << " needed=" << needed << " part=" << part);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(ErasureIsaLBackend) {
    Y_UNIT_TEST(MatrixAndDispatchedEncodeMatchIndependentAndBase) {
        const auto& codec = GetErasureIsaL();
        for (size_t len : {1u, 15u, 16u, 17u, 31u, 32u, 33u, 63u, 64u, 65u, 127u, 128u, 129u, 256u, 511u, 4097u}) {
            const auto original = MakeBuffers(len);
            auto actual = original;
            auto base = original;
            auto actualSlots = Slots(actual);
            auto baseSlots = Slots(base);
            std::array<const ui8*, 8> sources{};
            std::array<ui8*, 9> baseSources{};
            for (ui32 part = 0; part < 8; ++part) {
                sources[part] = actualSlots[part];
                baseSources[part] = baseSlots[part];
            }
            baseSources[8] = baseSources[7];
            std::array<ui8*, 2> outputs{actualSlots[8], actualSlots[9]};
            std::array<ui8*, 2> baseOutputs{baseSlots[8], baseSlots[9]};
            for (ui32 part = 8; part < 10; ++part) {
                std::fill_n(actualSlots[part], len, 0);
                std::fill_n(baseSlots[part], len, 0);
            }
            codec.Encode(len, sources.data(), outputs.data());
            alignas(64) std::array<ui8, 512> baseTables{};
            std::array<ui8, 16> coefficients{};
            for (ui32 row = 0; row < 2; ++row) {
                for (ui32 column = 0; column < 8; ++column) {
                    coefficients[row * 8 + column] = Matrix(row + 8, column);
                }
            }
            ec_init_tables_base(8, 2, coefficients.data(), baseTables.data());
            ec_encode_data_base(len, 8, 2, baseTables.data(), baseSources.data(), baseOutputs.data());
            UNIT_ASSERT_C(actual == original, "len=" << len);
            UNIT_ASSERT_C(actual == base, "len=" << len);
        }
    }

    Y_UNIT_TEST(All45BasesAnd135ExactPlansMatchIndependentCoefficients) {
        const auto& codec = GetErasureIsaL();
        ui32 bases = 0;
        ui32 plans = 0;
        for (ui32 first = 0; first < 10; ++first) {
            for (ui32 second = first + 1; second < 10; ++second) {
                const ui16 missing = (1u << first) | (1u << second);
                const auto& sources = codec.Sources(missing);
                const auto inverse = InvertBasis(sources);
                ++bases;
                for (ui16 needed : {ui16(1u << first), ui16(1u << second), missing}) {
                    std::array<std::array<ui8, 8>, 10> data{};
                    std::array<ui8*, 10> slots{};
                    for (ui32 part = 0; part < 10; ++part) {
                        slots[part] = data[part].data();
                    }
                    for (ui32 i = 0; i < 8; ++i) {
                        data[sources[i]][i] = 1;
                    }
                    codec.Restore(8, missing, needed, slots.data());
                    for (ui32 target : {first, second}) {
                        for (ui32 column = 0; column < 8; ++column) {
                            ui8 expected = 0;
                            if (needed & (1u << target)) {
                                for (ui32 j = 0; j < 8; ++j) {
                                    expected ^= Multiply(Matrix(target, j), inverse[j][column]);
                                }
                            }
                            UNIT_ASSERT_VALUES_EQUAL(data[target][column], expected);
                        }
                    }
                    ++plans;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(bases, TErasureIsaL::BasisCount);
        UNIT_ASSERT_VALUES_EQUAL(plans, TErasureIsaL::PlanCount);
    }

    Y_UNIT_TEST(All145LossOutputCasesAndUnalignedLengths) {
        for (size_t len : {1u, 15u, 16u, 17u, 31u, 32u, 33u, 63u, 64u, 65u, 127u, 128u, 129u, 256u, 511u, 4097u}) {
            const auto original = MakeBuffers(len);
            ui32 cases = 0;
            VerifyRestore(original, 0, 0, false);
            for (ui32 first = 0; first < 10; ++first) {
                const ui16 single = 1u << first;
                VerifyRestore(original, single, single, false);
                VerifyRestore(original, single, single, true);
                ++cases;
                for (ui32 second = first + 1; second < 10; ++second) {
                    const ui16 missing = single | (1u << second);
                    VerifyRestore(original, missing, 0, false);
                    for (ui16 needed : {single, ui16(1u << second), missing}) {
                        VerifyRestore(original, missing, needed, false);
                        ++cases;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(cases, 145);
        }
    }

    Y_UNIT_TEST(SingleLossSelectsFirstEightSurvivors) {
        for (ui32 lost = 0; lost < 10; ++lost) {
            const auto& sources = GetErasureIsaL().Sources(1u << lost);
            ui32 index = 0;
            for (ui32 part = 0; index < 8; ++part) {
                if (part != lost) {
                    UNIT_ASSERT_VALUES_EQUAL(sources[index++], part);
                }
            }
        }
    }

    Y_UNIT_TEST(EmptyOperationsNeedNoBuffers) {
        const auto& codec = GetErasureIsaL();
        codec.Encode(0, nullptr, nullptr);
        codec.Restore(0, 3, 3, nullptr);
        codec.Restore(64, 3, 0, nullptr);
        codec.Restore(64, 0, 0, nullptr);
    }

    Y_UNIT_TEST(ConcurrentRestoreUsesImmutablePlans) {
        std::atomic<bool> correct = true;
        std::array<std::thread, 8> threads;
        for (auto& thread : threads) {
            thread = std::thread([&] {
                const auto original = MakeBuffers(257);
                const auto& codec = GetErasureIsaL();
                for (ui32 first = 0; first < 10; ++first) {
                    for (ui32 second = first + 1; second < 10; ++second) {
                        auto buffers = original;
                        auto slots = Slots(buffers);
                        std::fill_n(slots[first], 257, 0);
                        std::fill_n(slots[second], 257, 0);
                        const ui16 missing = (1u << first) | (1u << second);
                        codec.Restore(257, missing, missing, slots.data());
                        if (buffers != original) {
                            correct.store(false, std::memory_order_relaxed);
                        }
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT(correct.load());
    }

#if defined(_linux_) && defined(_arm64_)
    Y_UNIT_TEST(AvailableNeonAndSveMatchBaseForEveryPlan) {
        // An SVE host must also validate NEON, although dispatch normally picks
        // only SVE there. These direct ISA entrypoints remain test-only.
        using TEncode = decltype(&ec_encode_data);
        std::vector<TEncode> implementations{ec_encode_data_base};
        const auto capabilities = getauxval(AT_HWCAP);
        if (capabilities & HWCAP_ASIMD) {
            implementations.push_back(ec_encode_data_neon);
        }
        if (capabilities & HWCAP_SVE) {
            implementations.push_back(ec_encode_data_sve);
        }
        for (size_t len : {1u, 15u, 16u, 17u, 31u, 32u, 33u, 63u, 64u, 65u}) {
            const auto original = MakeBuffers(len);
            for (ui32 first = 0; first < 10; ++first) {
                for (ui32 second = first + 1; second < 10; ++second) {
                    const auto& selection = GetErasureIsaL().Sources((1u << first) | (1u << second));
                    const auto inverse = InvertBasis(selection);
                    std::array<ui8, 16> coefficients{};
                    for (ui32 target = 0; target < 2; ++target) {
                        for (ui32 column = 0; column < 8; ++column) {
                            for (ui32 j = 0; j < 8; ++j) {
                                coefficients[target * 8 + column] ^= Multiply(Matrix(target ? second : first, j),
                                    inverse[j][column]);
                            }
                        }
                    }
                    for (auto encode : implementations) {
                        for (ui32 plan = 0; plan < 3; ++plan) {
                            const int rows = plan == 2 ? 2 : 1;
                            auto buffers = original;
                            auto slots = Slots(buffers);
                            std::array<ui8*, 9> sources{};
                            for (ui32 i = 0; i < 8; ++i) {
                                sources[i] = slots[selection[i]];
                            }
                            sources[8] = sources[7];
                            std::array<ui8*, 2> outputs{slots[plan == 1 ? second : first], slots[second]};
                            for (int i = 0; i < rows; ++i) {
                                std::fill_n(outputs[i], len, 0);
                            }
                            alignas(64) std::array<ui8, 512> tables{};
                            // Plan A deliberately uses the AB table prefix.
                            ec_init_tables_base(8, plan == 1 ? 1 : 2,
                                coefficients.data() + (plan == 1 ? 8 : 0), tables.data());
                            encode(len, 8, rows, tables.data(), sources.data(), outputs.data());
                            UNIT_ASSERT_C(buffers == original, "len=" << len << " first=" << first
                                << " second=" << second << " plan=" << plan);
                        }
                    }
                }
            }
        }
    }
#endif

#ifdef _unix_
    Y_UNIT_TEST(IsaL231SourceSentinelEndsAtGuardPage) {
        // On native SVE (or SVE QEMU), this catches assembly reads beyond
        // sources[8]. ASan alone cannot instrument ISA-L's raw assembly loads.
        const size_t pageSize = sysconf(_SC_PAGESIZE);
        void* mapping = mmap(nullptr, 2 * pageSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        UNIT_ASSERT(mapping != MAP_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(mprotect(static_cast<ui8*>(mapping) + pageSize, pageSize, PROT_NONE), 0);
        auto** sources = reinterpret_cast<ui8**>(static_cast<ui8*>(mapping) + pageSize) - 9;
        for (size_t len : {1u, 15u, 16u, 17u, 31u, 32u, 33u, 63u, 64u, 65u, 257u}) {
            const auto original = MakeBuffers(len);
            auto buffers = original;
            auto slots = Slots(buffers);
            for (ui32 i = 0; i < 8; ++i) {
                sources[i] = slots[i];
            }
            sources[8] = sources[7];
            for (int rows : {1, 2}) {
                alignas(64) std::array<ui8, 512> tables{};
                std::array<ui8, 16> coefficients{};
                for (ui32 i = 0; i < 16; ++i) {
                    coefficients[i] = Matrix(8 + i / 8, i % 8);
                }
                std::array<ui8*, 2> outputs{slots[8], slots[9]};
                std::fill_n(outputs[0], len, 0);
                if (rows == 2) {
                    std::fill_n(outputs[1], len, 0);
                }
                ec_init_tables(8, rows, coefficients.data(), tables.data());
                ec_encode_data(len, 8, rows, tables.data(), sources, outputs.data());
                UNIT_ASSERT(buffers == original);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(munmap(mapping, 2 * pageSize), 0);
    }
#endif
}

} // namespace NKikimr
