#include <ydb/core/tx/datashard/multi_txids.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <array>

namespace {

using namespace NKikimr::NDataShard;
using NKikimr::NTable::ELockMode;

constexpr size_t MaxSteps = 256;

ELockMode PickMode(FuzzedDataProvider& provider, bool includeMulti = false) {
    static constexpr std::array<ELockMode, 5> Modes = {
        ELockMode::None,
        ELockMode::KeyShared,
        ELockMode::Shared,
        ELockMode::NoKeyExclusive,
        ELockMode::Exclusive,
    };
    if (includeMulti && provider.ConsumeIntegralInRange<unsigned>(0, 15) == 0) {
        return ELockMode::Multi;
    }
    return Modes[provider.ConsumeIntegralInRange<size_t>(0, Modes.size() - 1)];
}

bool CompatibilityModel(ELockMode currentMode, ELockMode lockMode) {
    switch (currentMode) {
        case ELockMode::None:
            return lockMode < ELockMode::Multi;
        case ELockMode::KeyShared:
            return lockMode < ELockMode::Exclusive;
        case ELockMode::Shared:
            return lockMode < ELockMode::NoKeyExclusive;
        case ELockMode::NoKeyExclusive:
            return lockMode < ELockMode::Shared;
        default:
            return false;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t i = 0; i < steps && provider.remaining_bytes() > 0; ++i) {
        const ELockMode a = PickMode(provider, true);
        const ELockMode b = PickMode(provider, true);
        const ELockMode c = PickMode(provider, true);

        Y_ABORT_UNLESS(IsCompatibleRowLockMode(a, b) == CompatibilityModel(a, b));
        Y_ABORT_UNLESS(IsCompatibleRowLockMode(b, a) == CompatibilityModel(b, a));

        if (a != ELockMode::Multi && b != ELockMode::Multi) {
            const ELockMode ab = CombinedRowLockMode(a, b);
            const ELockMode ba = CombinedRowLockMode(b, a);
            Y_ABORT_UNLESS(ab == ba);
            Y_ABORT_UNLESS(ab >= a);
            Y_ABORT_UNLESS(ab >= b);
            if (c != ELockMode::Multi) {
                Y_ABORT_UNLESS(CombinedRowLockMode(CombinedRowLockMode(a, b), c) ==
                               CombinedRowLockMode(a, CombinedRowLockMode(b, c)));
            }
        }
    }

    return 0;
}
