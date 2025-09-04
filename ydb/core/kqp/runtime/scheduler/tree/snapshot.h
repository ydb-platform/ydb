#pragma once

#include "common.h"

#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

    struct TTreeElement : public virtual TTreeElementBase<ETreeType::SNAPSHOT> {
        ui64 TotalLimit = Infinity();
        ui64 FairShare = 0;

        ui64 Demand = 0;
        ui64 Usage = 0;
        std::optional<float> Satisfaction;

        const TMonotonic Timestamp = TMonotonic::Now();

        explicit TTreeElement(const TId& id, const TStaticAttributes& attrs = {}) : TTreeElementBase(id, attrs) {}

        TPool* GetParent() const;

        virtual void AccountSnapshotDuration(const TDuration& period);
        virtual void UpdateBottomUp(ui64 totalLimit);
        void UpdateTopDown();
    };

    class TQuery : public TTreeElement, public NHdrf::TQuery<ETreeType::SNAPSHOT>, public std::enable_shared_from_this<TQuery> {
    public:
        TQuery(const TQueryId& queryId, NDynamic::TQueryPtr query);

        std::weak_ptr<NDynamic::TQuery> Origin; // TODO: why public?
    };

    class TPool : public TTreeElement, public NHdrf::TPool<ETreeType::SNAPSHOT> {
    public:
        TPool(const TPoolId& id, const std::optional<TPoolCounters>& counters, const TStaticAttributes& attrs = {});

        void AccountSnapshotDuration(const TDuration& period) override;
        void UpdateBottomUp(ui64 totalLimit) override;
    };

    class TDatabase : public TPool {
    public:
        explicit TDatabase(const TDatabaseId& id, const TStaticAttributes& attrs = {});
    };

    class TRoot : public TPool {
    public:
        TRoot();

        inline bool IsRoot() const final {
            return true;
        }

        void AddDatabase(const TDatabasePtr& database);
        void RemoveDatabase(const TDatabaseId& databaseId);
        TDatabasePtr GetDatabase(const TDatabaseId& databaseId) const;

        void AccountPreviousSnapshot(const TRootPtr& snapshot);
    };

}
