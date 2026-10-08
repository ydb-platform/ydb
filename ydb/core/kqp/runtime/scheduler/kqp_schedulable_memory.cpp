#include "kqp_schedulable_memory.h"

#include "kqp_compute_scheduler_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>

#include <algorithm>
#include <limits>

namespace NKikimr::NKqp::NScheduler {

using namespace NHdrf::NDynamic;

namespace {

TRoot* FindRoot(TTreeElement* element) {
    Y_ENSURE(element);
    while (auto* parent = element->GetParent()) {
        element = parent;
    }

    auto* root = dynamic_cast<TRoot*>(element);
    Y_ENSURE(root, "The pool is not attached to the root");
    return root;
}

// The part of the limit given for the elastic growth, see TSchedulableMemory::ElasticMemoryPercent.
// The infinite limit stays infinite.
ui64 GetElasticPart(ui64 limit, double elasticMemoryPercent) {
    if (limit == NHdrf::Infinity()) {
        return limit;
    }

    // Exact for any ui64 value
    const long double elasticLimit = static_cast<long double>(limit) * elasticMemoryPercent / 100;
    return elasticLimit >= limit ? limit : static_cast<ui64>(elasticLimit);
}

bool TryIncrease(std::atomic<ui64>& usage, ui64 bytes, ui64 limit) {
    if (bytes > limit) {
        return false;
    }

    // The usage may already exceed the limit, since memory can't be taken back.
    const ui64 maxUsage = limit - bytes;
    ui64 current = usage.load();
    bool increased = false;

    while (!increased && current <= maxUsage) {
        increased = usage.compare_exchange_weak(current, current + bytes);
    }

    return increased;
}

i64 CalculateAvailability(ui64 usage, ui64 limit) {
    constexpr ui64 maxAvailability = std::numeric_limits<i64>::max();

    if (limit == NHdrf::Infinity()) {
        return maxAvailability;
    }
    if (limit >= usage) {
        return static_cast<i64>(Min(limit - usage, maxAvailability));
    }
    return -static_cast<i64>(Min(usage - limit, maxAvailability));
}

} // namespace

TSchedulableMemory::TSchedulableMemory(const TQueryPtr& query, double elasticMemoryPercent)
    : Leaf(query)
    , Pool(query->GetParent())
    , Root(FindRoot(Pool))
    , ElasticMemoryPercent(std::clamp(elasticMemoryPercent, 0.0, 100.0))
{
}

TSchedulableMemory::TSchedulableMemory(const TPoolPtr& pool, double elasticMemoryPercent)
    : Leaf(pool)
    , Pool(pool.get())
    , Root(FindRoot(Pool))
    , ElasticMemoryPercent(std::clamp(elasticMemoryPercent, 0.0, 100.0))
{
}

TTreeElement* TSchedulableMemory::GetLeaf() const {
    return std::visit([](const auto& leaf) -> TTreeElement* { return leaf.get(); }, Leaf);
}

TQuery* TSchedulableMemory::GetQuery() const {
    const auto* query = std::get_if<TQueryPtr>(&Leaf);
    return query ? query->get() : nullptr;
}

ui64 TSchedulableMemory::GetPoolLimit() const {
    const auto* query = GetQuery();
    if (!query) {
        return NHdrf::Infinity();
    }

    // Like the CPU: the fair-share of the pool comes with the latest snapshot of the query, the new query gets it from
    // the snapshot of the pool, see TQuery::InitSnapshot. Before the pool gets into any snapshot it's not limited,
    // the default pool is never limited - see NSnapshot::TDefaultPool.
    const auto snapshot = query->GetSnapshot();
    return snapshot ? snapshot->MemoryFairShare : NHdrf::Infinity();
}

bool TSchedulableMemory::TryIncreaseUsage(ui64 bytes, bool optional) {
    if (!bytes) {
        return true;
    }

    // The optional memory is given only within the elastic part of the limits
    const double elasticPercent = optional ? ElasticMemoryPercent : 100;

    if (const ui64 poolLimit = GetPoolLimit(); poolLimit == NHdrf::Infinity()) {
        Pool->MemoryUsage += bytes;
    } else if (!TryIncrease(Pool->MemoryUsage, bytes, GetElasticPart(poolLimit, elasticPercent))) {
        return false;
    }

    if (!TryIncrease(Root->MemoryUsage, bytes, GetElasticPart(Root->TotalMemoryLimit.load(), elasticPercent))) {
        Pool->MemoryUsage -= bytes;
        return false;
    }

    if (auto* query = GetQuery()) {
        query->MemoryUsage += bytes;
    }
    for (TPool* parent = Pool->GetParent(); parent != Root; parent = parent->GetParent()) {
        parent->MemoryUsage += bytes;
    }

    return true;
}

void TSchedulableMemory::IncreaseUsage(ui64 bytes) {
    for (TTreeElement* parent = GetLeaf(); parent; parent = parent->GetParent()) {
        parent->MemoryUsage += bytes;
    }
}

void TSchedulableMemory::IncreaseDemand(ui64 bytes) {
    if (auto* query = GetQuery()) {
        query->MemoryDemand += bytes;
    }
    Root->MemoryDemand += bytes;
}

void TSchedulableMemory::DecreaseDemand(ui64 bytes) {
    if (auto* query = GetQuery()) {
        [[maybe_unused]] const auto prevQueryDemand = query->MemoryDemand.fetch_sub(bytes);
        Y_DEBUG_ABORT_UNLESS(prevQueryDemand >= bytes);
    }
    [[maybe_unused]] const auto prevRootDemand = Root->MemoryDemand.fetch_sub(bytes);
    Y_DEBUG_ABORT_UNLESS(prevRootDemand >= bytes);
}

void TSchedulableMemory::DecreaseUsage(ui64 bytes) {
    for (TTreeElement* parent = GetLeaf(); parent; parent = parent->GetParent()) {
        [[maybe_unused]] const auto prevUsage = parent->MemoryUsage.fetch_sub(bytes);
        Y_DEBUG_ABORT_UNLESS(prevUsage >= bytes);
    }
}

i64 TSchedulableMemory::GetAvailability() const {
    i64 availability = CalculateAvailability(
        Root->MemoryUsage.load(std::memory_order_relaxed), GetElasticPart(Root->TotalMemoryLimit.load(), ElasticMemoryPercent));

    if (const ui64 poolLimit = GetPoolLimit(); poolLimit != NHdrf::Infinity()) {
        availability = Min(availability, CalculateAvailability(
            Pool->MemoryUsage.load(std::memory_order_relaxed), GetElasticPart(poolLimit, ElasticMemoryPercent)));
    }

    return availability;
}

TSchedulableMemoryPtr CreateSchedulableMemory(const TQueryPtr& query, const NHdrf::TDatabaseId& databaseId,
    const NHdrf::TPoolId& poolId, double elasticMemoryPercent)
{
    if (query) {
        return std::make_shared<TSchedulableMemory>(query, elasticMemoryPercent);
    }
    if (const auto& scheduler = AppData()->KqpComputeScheduler) {
        return std::make_shared<TSchedulableMemory>(scheduler->GetOrCreateMemoryPool(
            databaseId.empty() ? CanonizePath(AppData()->TenantName) : databaseId, poolId), elasticMemoryPercent);
    }
    return nullptr;
}

} // namespace NKikimr::NKqp::NScheduler
