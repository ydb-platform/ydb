#include "blobstorage_ingress_matrix.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/platform.h>

#include <array>
#include <vector>

#ifdef _unix_
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace NKikimr::NMatrix {
namespace {

constexpr std::array<ui16, 16> RawBits{
    0x0080, 0x0040, 0x0020, 0x0010, 0x0008, 0x0004, 0x0002, 0x0001,
    0x8000, 0x4000, 0x2000, 0x1000, 0x0800, 0x0400, 0x0200, 0x0100,
};

ui16 EncodeRaw(ui32 logical, ui8 size) {
    ui16 raw = 0;
    for (ui8 i = 0; i < size; ++i) {
        if (logical & (1u << i)) {
            raw |= RawBits[i];
        }
    }
    return raw;
}

TVectorType FromLogical(ui32 logical, ui8 size) {
    TVectorType result(0, size);
    for (ui8 i = 0; i < size; ++i) {
        if (logical & (1u << i)) {
            result.Set(i);
        }
    }
    return result;
}

void CheckVector(const TVectorType& actual, ui32 logical, ui8 size) {
    UNIT_ASSERT_VALUES_EQUAL(actual.GetSize(), size);
    UNIT_ASSERT_VALUES_EQUAL(actual.Raw(), EncodeRaw(logical, size));
    UNIT_ASSERT_VALUES_EQUAL(actual.CountBits(), std::popcount(logical));
    UNIT_ASSERT_VALUES_EQUAL(actual.Empty(), !logical);
    UNIT_ASSERT(actual == TVectorType(actual.Raw(), size));
    ui8 before = 0;
    std::vector<ui8> expected;
    for (ui8 i = 0; i < size; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual.BitsBefore(i), before);
        UNIT_ASSERT_VALUES_EQUAL(actual.Get(i), bool(logical & (1u << i)));
        if (logical & (1u << i)) {
            ++before;
            expected.push_back(i);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(actual.BitsBefore(size), before);
    UNIT_ASSERT_VALUES_EQUAL(actual.FirstPosition(), expected.empty() ? size : expected.front());
    std::vector<ui8> iterated;
    for (ui8 part : actual) {
        iterated.push_back(part);
    }
    UNIT_ASSERT(iterated == expected);
    for (ui8 i = 0; i < size; ++i) {
        ui8 next = i + 1;
        while (next < size && !(logical & (1u << next))) {
            ++next;
        }
        UNIT_ASSERT_VALUES_EQUAL(actual.NextPosition(i), next);
    }
}

#ifdef _unix_
template<class TAction>
void AssertAborts(TAction action) {
    const pid_t pid = fork();
    UNIT_ASSERT(pid >= 0);
    if (!pid) {
        const rlimit noCore{0, 0};
        setrlimit(RLIMIT_CORE, &noCore);
        signal(SIGABRT, SIG_DFL);
        if (!freopen("/dev/null", "w", stderr)) {
            _exit(100);
        }
        action();
        _exit(0);
    }
    int status = 0;
    pid_t result;
    do {
        result = waitpid(pid, &status, 0);
    } while (result < 0 && errno == EINTR);
    UNIT_ASSERT_VALUES_EQUAL(result, pid);
    UNIT_ASSERT(WIFSIGNALED(status));
    UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
}
#endif

} // anonymous namespace

Y_UNIT_TEST_SUITE(IngressWideVector) {
    Y_UNIT_TEST(LegacyRaw8EverySizeAndBytePattern) {
        for (ui8 size = 0; size <= 8; ++size) {
            const ui8 validMask = (0xffu << (8 - size)) & 0xffu;
            for (ui32 raw = 0; raw <= 255; ++raw) {
                const TVectorType vector(raw, size);
                UNIT_ASSERT_VALUES_EQUAL(vector.Raw8(), raw & validMask);
                UNIT_ASSERT_VALUES_EQUAL(vector.Raw(), vector.Raw8());
                for (ui8 i = 0; i < size; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(vector.Get(i), bool(raw & (0x80u >> i)));
                }
            }
        }
    }

    Y_UNIT_TEST(EveryLogicalPatternUpTo16Parts) {
        for (ui8 size = 0; size <= 16; ++size) {
            for (ui32 pattern = 0; pattern < (1u << size); ++pattern) {
                CheckVector(FromLogical(pattern, size), pattern, size);
            }
        }
    }

    Y_UNIT_TEST(SetClearOneHotAndValidMasks) {
        UNIT_ASSERT_VALUES_EQUAL(TVectorType(0xffff, 8).Raw(), 0x00ff);
        UNIT_ASSERT_VALUES_EQUAL(TVectorType(0xffff, 9).Raw(), 0x80ff);
        UNIT_ASSERT_VALUES_EQUAL(TVectorType(0xffff, 10).Raw(), 0xc0ff);
        UNIT_ASSERT_VALUES_EQUAL(TVectorType(0xffff, 16).Raw(), 0xffff);
        for (ui8 size = 1; size <= 16; ++size) {
            TVectorType vector(0, size);
            for (ui8 part = 0; part < size; ++part) {
                vector.Set(part);
                CheckVector(vector, 1u << part, size);
                UNIT_ASSERT(vector == TVectorType::MakeOneHot(part, size));
                vector.Clear(part);
                CheckVector(vector, 0, size);
            }
        }
        const TVectorType empty;
        UNIT_ASSERT(empty.ToString().empty());
        UNIT_ASSERT(empty.begin() == empty.end());
        auto vector = FromLogical(0x301, 10);
        UNIT_ASSERT_VALUES_EQUAL(vector.ToString(), "1 0 0 0 0 0 0 0 1 1");
        vector.Clear();
        CheckVector(vector, 0, 10);
    }

    Y_UNIT_TEST(OperatorsOnDeterministicWidePatterns) {
        ui32 random = 0x17f96da1;
        auto next = [&] {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            return random;
        };
        for (ui8 size = 0; size <= 16; ++size) {
            const ui32 valid = (1u << size) - 1;
            for (ui32 sample = 0; sample < 256; ++sample) {
                const ui32 left = next() & valid;
                const ui32 right = next() & valid;
                const auto a = FromLogical(left, size);
                const auto b = FromLogical(right, size);
                CheckVector(a | b, left | right, size);
                CheckVector(a & b, left & right, size);
                CheckVector(a - b, left & ~right, size);
                CheckVector(~a, ~left & valid, size);
                UNIT_ASSERT_VALUES_EQUAL(a == b, left == right);
                UNIT_ASSERT_VALUES_EQUAL(a != b, left != right);
                UNIT_ASSERT_VALUES_EQUAL(a.IsSupersetOf(b), (left & right) == right);
                auto assigned = a;
                assigned |= b;
                UNIT_ASSERT(assigned == (a | b));
                assigned = a;
                assigned &= b;
                UNIT_ASSERT(assigned == (a & b));
                assigned = a;
                assigned -= b;
                UNIT_ASSERT(assigned == (a - b));
                auto swapped = b;
                assigned = a;
                assigned.Swap(swapped);
                UNIT_ASSERT(assigned == b);
                UNIT_ASSERT(swapped == a);
            }
        }
    }

    Y_UNIT_TEST(ShiftedConvertersAcrossByteBoundaries) {
        for (ui8 size = 1; size <= 16; ++size) {
            for (ui8 begin : {ui8(0), ui8(2), ui8(7), ui8(17)}) {
                std::array<ui8, 8> bytes{};
                TShiftedMainBitVec main(bytes.data(), begin, begin + size);
                for (ui8 part = 0; part < size; ++part) {
                    main.Set(part);
                    CheckVector(main.ToVector(), (1u << (part + 1)) - 1, size);
                }
                for (ui8 part = 0; part < size; ++part) {
                    main.Clear(part);
                    CheckVector(main.ToVector(), ((1u << size) - 1) & ~((1u << (part + 1)) - 1), size);
                }
                for (ui8 part = 0; part < size; ++part) {
                    for (ui8 state = 0; state < 4; ++state) {
                        bytes.fill(0);
                        TShiftedHandoffBitVec handoff(bytes.data(), begin, begin + 2 * size);
                        if (state & 1) {
                            handoff.Set(part);
                        }
                        if (state & 2) {
                            handoff.Delete(part);
                        }
                        UNIT_ASSERT_VALUES_EQUAL(handoff.GetRaw(part), state);
                        CheckVector(handoff.ToVector(), state == 1 ? 1u << part : 0, size);
                        CheckVector(handoff.DeletedPartsVector(), state & 2 ? 1u << part : 0, size);
                    }
                }
            }
        }
    }

#ifdef _unix_
    Y_UNIT_TEST(Raw8RejectsWideSizeEvenWithoutHighBits) {
        for (ui8 size = 9; size <= 16; ++size) {
            AssertAborts([=] { TVectorType(0, size).Raw8(); });
            AssertAborts([=] { TVectorType(0xff, size).Raw8(); });
        }
        AssertAborts([] { TVectorType(0, 17); });
    }
#endif
}

} // namespace NKikimr::NMatrix
