#pragma once

#include <ydb/core/tx/replication/common/worker_id.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

#include <optional>
#include <utility>

namespace NKikimr::NReplication::NController {

class TWorkerLagExpiryQueue {
    using TDeadline = NMonotonic::TMonotonic;
    using TExpirations = TList<std::pair<TDeadline, TWorkerId>>;

public:
    TWorkerLagExpiryQueue() = default;
    TWorkerLagExpiryQueue(const TWorkerLagExpiryQueue&) = delete;
    TWorkerLagExpiryQueue& operator=(const TWorkerLagExpiryQueue&) = delete;

    // Deadlines must arrive in nondecreasing order. The controller derives them
    // from monotonic receipt times plus the same timeout for every worker.
    void SetDeadline(const TWorkerId& id, TDeadline deadline);

    void Remove(const TWorkerId& id);

    std::optional<TDeadline> GetNextDeadline() const;

    std::optional<TWorkerId> PopExpired(TDeadline now);

    size_t GetSize() const;

    void Clear();

private:
    TExpirations Expirations;
    THashMap<TWorkerId, TExpirations::iterator> WorkerEntries;
};

}
