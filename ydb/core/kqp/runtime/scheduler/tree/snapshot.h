#pragma once

#include "common.h"

#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

    struct TTreeElement : public virtual TTreeElementBase<ETreeType::SNAPSHOT> {
        ui64 FairShare = 0;

        std::atomic<ui64> CpuMaxDemand = 0;

        ui64 CpuActualDemand = 0;
        ui64 PreciseCpuActualDemand = 0; // in micro-cores

        ui64 CpuBurstUsage = 0;
        ui64 CpuBurstThrottle = 0;

        explicit TTreeElement(const TId& id, const TStaticAttributes& attrs = {}) : TTreeElementBase(id, attrs) {}

        TPool* GetParent() const;

        virtual void AccountSnapshotDuration(TDuration period);
        virtual void UpdateBottomUp(ui64 totalLimit, TDuration period);
        void UpdateTopDown();

    private:
        void DistributeFairShare();
    };

    class TQuery : public TTreeElement, public NHdrf::TQuery<ETreeType::SNAPSHOT>, public std::enable_shared_from_this<TQuery> {
    public:
        TQuery(const TQueryId& queryId, const NDynamic::TQueryPtr& query);

        void UpdateBottomUp(ui64 totalLimit, TDuration period) override;

        std::weak_ptr<NDynamic::TQuery> Origin; // TODO: why public?

        ui64 CpuUsage = 0;
        ui64 CpuThrottle = 0;

        // The actual demand before smoothing, in micro-cores - PreciseCpuActualDemand is the max of these two.
        ui64 RawCpuActualDemand = 0;     // measured over the latest period by UpdateBottomUp(), read by the next snapshot
        ui64 PrevRawCpuActualDemand = 0; // copied from the previous snapshot by NDynamic::TQuery::TakeSnapshot()

    private:
        ui64 CalculateRawCpuActualDemand(TDuration period) const;
    };

    class TPool : public TTreeElement, public NHdrf::TPool<ETreeType::SNAPSHOT> {
    public:
        TPool(const TPoolId& id, const std::optional<TPoolCounters>& counters, const TStaticAttributes& attrs = {});

        void AccountSnapshotDuration(TDuration period) override;
    };

    class TDatabase : public TPool {
    public:
        explicit TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs = {});
    };

    class TRoot : public TPool {
    public:
        TRoot();

        bool IsRoot() const final {
            return true;
        }

        void AddDatabase(const TDatabasePtr& database);
        void RemoveDatabase(const TDatabaseId& databaseId);
        TDatabasePtr GetDatabase(const TDatabaseId& databaseId) const;

        // Calculates the snapshot relative to the previous one
        void Update(const TRootPtr& previous);

    public:
        const TMonotonic Timestamp = TMonotonic::Now();
        ui64 TotalLimit = Infinity();
    };

} // namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot
