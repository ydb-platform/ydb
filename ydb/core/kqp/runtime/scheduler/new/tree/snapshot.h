#pragma once

#include "../fwd.h"
#include "common.h"

#include <library/cpp/time_provider/monotonic.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <atomic>
#include <vector>

namespace NKikimr::NKqp::NScheduler::NHdrf::NSnapshot {

    struct TTreeElementBase : public TStaticAttributes {
        ui64 TotalLimit = Infinity();

        ui64 FairShare = 0; // dynamic?+snapshot

        std::atomic<ui64> Demand = 0; // dynamic+snapshot

        TTreeElementBase* Parent = nullptr; // dynamic+snapshot
        std::vector<TTreeElementPtr> Children; // dynamic+snapshot

        virtual ~TTreeElementBase() = default;

        void AddChild(const TTreeElementPtr& element); // dynamic+snapshot
        void RemoveChild(const TTreeElementPtr& element); // dynamic+snapshot

        bool IsRoot() const {
            return !Parent;
        }

        bool IsLeaf() const {
            return Children.empty();
        }

        virtual void AccountFairShare(const TDuration& period); // snapshot
        virtual void UpdateBottomUp(ui64 totalLimit); // snapshot
        void UpdateTopDown(); // snapshot
    };

    class TQuery : public TTreeElementBase {
    public:
        explicit TQuery(const TQueryId& queryId, const TStaticAttributes& attrs = {});

        const TQueryId& GetId() const {
            return Id;
        }

    private:
        const TQueryId Id;
    };

    class TPool : public TTreeElementBase {
    public:
        TPool(const TString& id, const TPoolCounters& counters, const TStaticAttributes& attrs = {});

        const TString& GetId() const {
            return Id;
        }

        void AccountFairShare(const TDuration& period) override;
        void UpdateBottomUp(ui64 totalLimit) override;

        void AddQuery(const TQueryPtr& query);
        void RemoveQuery(const TQueryId& queryId);
        TQueryPtr GetQuery(const TQueryId& queryId) const;

    private:
        const TString Id;
        THashMap<TQueryId, TQueryPtr> Queries;
        TPoolCounters Counters;
    };

    class TDatabase : public TTreeElementBase {
    public:
        explicit TDatabase(const TString& id, const TStaticAttributes& attrs = {});

        void AddPool(const TPoolPtr& pool);
        TPoolPtr GetPool(const TString& poolId) const;

        const TString& GetId() const {
            return Id;
        }

    private:
        const TString Id;
        THashMap<TString /* poolId */, TPoolPtr> Pools;
    };

    class TRoot : public TTreeElementBase {
    public:
        void AddDatabase(const TDatabasePtr& database);
        TDatabasePtr GetDatabase(const TString& id) const;

        void AccountFairShare(const TRootPtr& previous);
        using TTreeElementBase::AccountFairShare;

    private:
        const TMonotonic Timestamp = TMonotonic::Now();
        THashMap<TString /* databaseId */, TDatabasePtr> Databases;
    };

}
