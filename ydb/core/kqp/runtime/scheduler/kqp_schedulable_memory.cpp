#include "kqp_schedulable_memory.h"

#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <limits>

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

TSchedulableMemory::TSchedulableMemory(const TQueryPtr& query)
    : Query(query)
{
    Y_ENSURE(query);
}

bool TSchedulableMemory::TryIncreaseUsage(ui64 bytes) {
    // The query snapshot inherits the fair-share of the pool, since it doesn't keep the parent snapshot alive.
    const auto snapshot = Query->GetSnapshot();
    if (!snapshot) {
        IncreaseUsage(bytes);
        return true;
    }

    const ui64 fairShare = snapshot->MemoryFairShare;
    if (bytes > fairShare) {
        return false;
    }

    // The usage may already exceed the fair-share, since memory can't be taken back.
    const ui64 maxUsage = fairShare - bytes;
    auto* pool = Query->GetParent();
    ui64 usage = pool->MemoryUsage.load();
    bool increased = false;

    while (!increased && usage <= maxUsage) {
        increased = pool->MemoryUsage.compare_exchange_weak(usage, usage + bytes);
    }

    if (!increased) {
        return false;
    }

    Query->MemoryUsage += bytes;
    for (TTreeElement* parent = pool->GetParent(); parent; parent = parent->GetParent()) {
        parent->MemoryUsage += bytes;
    }

    return true;
}

void TSchedulableMemory::IncreaseUsage(ui64 bytes) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        parent->MemoryUsage += bytes;
    }
}

void TSchedulableMemory::DecreaseUsage(ui64 bytes) {
    for (TTreeElement* parent = Query.get(); parent; parent = parent->GetParent()) {
        [[maybe_unused]] const auto prevUsage = parent->MemoryUsage.fetch_sub(bytes);
        Y_DEBUG_ABORT_UNLESS(prevUsage >= bytes);
    }
}

i64 TSchedulableMemory::GetAvailability() const {
    const auto snapshot = Query->GetSnapshot();
    if (!snapshot) {
        return std::numeric_limits<i64>::max();
    }

    const ui64 fairShare = snapshot->MemoryFairShare;
    const ui64 usage = Query->GetParent()->MemoryUsage.load(std::memory_order_relaxed);
    constexpr ui64 maxAvailability = std::numeric_limits<i64>::max();

    if (fairShare >= usage) {
        return static_cast<i64>(Min(fairShare - usage, maxAvailability));
    }
    return -static_cast<i64>(Min(usage - fairShare, maxAvailability));
}

} // namespace NKikimr::NKqp::NScheduler
