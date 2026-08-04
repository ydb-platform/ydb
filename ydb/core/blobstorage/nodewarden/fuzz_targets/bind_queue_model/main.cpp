#include <ydb/core/blobstorage/nodewarden/bind_queue.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr::NStorage;

constexpr ui32 MaxOps = 256;
constexpr ui32 MaxNodes = 24;
constexpr ui32 MaxNodeId = 64;

TVector<ui32> Sorted(const THashSet<ui32>& values) {
    TVector<ui32> out(values.begin(), values.end());
    std::sort(out.begin(), out.end());
    return out;
}

ui32 PickFrom(const TVector<ui32>& values, FuzzedDataProvider& provider) {
    Y_ABORT_UNLESS(!values.empty());
    return values[provider.ConsumeIntegralInRange<size_t>(0, values.size() - 1)];
}

void CheckQueueEmpty(const TBindQueue& queue, const THashSet<ui32>& present) {
    Y_ABORT_UNLESS(queue.Empty() == present.empty());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    TBindQueue queue;
    THashSet<ui32> present;
    THashSet<ui32> enabled;
    THashSet<ui32> disabled;
    TMonotonic now = TMonotonic::Zero();

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 i = 0; i < ops; ++i) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 4)) {
            case 0: {
                THashSet<ui32> next;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(0, MaxNodes);
                for (ui32 j = 0; j < count; ++j) {
                    next.insert(provider.ConsumeIntegralInRange<ui32>(1, MaxNodeId));
                }

                queue.Update(Sorted(next));
                present = std::move(next);

                TVector<ui32> removedDisabled;
                for (ui32 nodeId : disabled) {
                    if (!present.contains(nodeId)) {
                        removedDisabled.push_back(nodeId);
                    }
                }
                for (ui32 nodeId : removedDisabled) {
                    disabled.erase(nodeId);
                }

                enabled.clear();
                for (ui32 nodeId : present) {
                    if (!disabled.contains(nodeId)) {
                        enabled.insert(nodeId);
                    }
                }
                break;
            }
            case 1: {
                const auto values = Sorted(enabled);
                if (!values.empty()) {
                    const ui32 nodeId = PickFrom(values, provider);
                    queue.Disable(nodeId);
                    enabled.erase(nodeId);
                    disabled.insert(nodeId);
                }
                break;
            }
            case 2: {
                const auto values = Sorted(disabled);
                if (!values.empty()) {
                    const ui32 nodeId = PickFrom(values, provider);
                    queue.Enable(nodeId);
                    disabled.erase(nodeId);
                    enabled.insert(nodeId);
                }
                break;
            }
            case 3: {
                TMonotonic closest = TMonotonic::Max();
                const auto picked = queue.Pick(now, &closest);
                if (picked) {
                    Y_ABORT_UNLESS(enabled.contains(*picked));
                    Y_ABORT_UNLESS(present.contains(*picked));
                } else if (enabled.empty()) {
                    Y_ABORT_UNLESS(closest == TMonotonic::Max());
                }
                break;
            }
            default:
                now += TDuration::MilliSeconds(provider.ConsumeIntegralInRange<ui32>(0, 2500));
                break;
        }

        Y_ABORT_UNLESS(disabled.size() + enabled.size() == present.size());
        for (ui32 nodeId : disabled) {
            Y_ABORT_UNLESS(present.contains(nodeId));
            Y_ABORT_UNLESS(!enabled.contains(nodeId));
        }
        CheckQueueEmpty(queue, present);
    }

    return 0;
}
