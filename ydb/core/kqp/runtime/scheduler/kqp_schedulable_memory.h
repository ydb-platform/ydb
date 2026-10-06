#pragma once

#include "fwd.h"

#include <variant>

namespace NKikimr::NKqp::NScheduler {

// The proxy-object between the memory consumers of a tx and the scheduler - like TSchedulableTask for the CPU.
// The usage is accounted for the query and all its parents. It's limited by the fair-share of the pool (the default
// pool has the infinite one) and by the total limit of the root: the latter is checked on every increase, so that
// the memory of all the queries on the node never exceeds it - independently of the snapshots. Thread safe.

class TSchedulableMemory {
public:
    // The elastic memory percent is the part of the limits - of the fair-share of the pool and of the total limit -
    // given for the elastic growth of the tasks (E): the optional memory is given only within it, and the availability
    // is the bytes left in it. The rest is kept for the mandatory memory. The scheduler itself knows nothing about it.
    // TODO: the same elastic part for every task of the tx - give each task its own budget of E instead.
    explicit TSchedulableMemory(const NHdrf::NDynamic::TQueryPtr& query, double elasticMemoryPercent = 100);

    // Without the query - for the cheap accounting of the default pool, which may have no query nodes.
    // Then the fair-share of the pool isn't checked, only the total limit.
    explicit TSchedulableMemory(const NHdrf::NDynamic::TPoolPtr& pool, double elasticMemoryPercent = 100);

    // Refused when the memory doesn't fit into the fair-share of the pool or into the total limit; the optional
    // memory - into their elastic parts.
    bool TryIncreaseUsage(ui64 bytes, bool optional = false);
    void DecreaseUsage(ui64 bytes);

    // Increases the usage unconditionally - may exceed any limit.
    void IncreaseUsage(ui64 bytes);

    // The expected memory - accounted for the query (the snapshot sums it up) and for the root (its live total).
    void IncreaseDemand(ui64 bytes);
    void DecreaseDemand(ui64 bytes);

    // The bytes left in the elastic part of the pool fair-share or of the total limit, whichever is less.
    // Negative on overuse, |value| is the overuse.
    i64 GetAvailability() const;

private:
    NHdrf::NDynamic::TTreeElement* GetLeaf() const;
    // nullptr without the query
    NHdrf::NDynamic::TQuery* GetQuery() const;
    // The fair-share of the pool, Infinity() if the pool isn't limited
    ui64 GetPoolLimit() const;

private:
    // The query, if there is one, otherwise the pool itself. The query keeps the pointer to its pool even after it's
    // removed from the tree, and the pools are never removed.
    const std::variant<NHdrf::NDynamic::TQueryPtr, NHdrf::NDynamic::TPoolPtr> Leaf;
    NHdrf::NDynamic::TPool* const Pool;
    NHdrf::NDynamic::TRoot* const Root; // owned by the scheduler, which outlives any tx
    const double ElasticMemoryPercent;
};

using TSchedulableMemoryPtr = std::shared_ptr<TSchedulableMemory>;

// The memory of a tx: of its query node, if there is one, otherwise of its pool - an empty database means the database
// of the node, an empty pool - the default one. Without the scheduler - nullptr. Must be called from an actor.
TSchedulableMemoryPtr CreateSchedulableMemory(const NHdrf::NDynamic::TQueryPtr& query, const NHdrf::TDatabaseId& databaseId,
    const NHdrf::TPoolId& poolId, double elasticMemoryPercent);

} // namespace NKikimr::NKqp::NScheduler
