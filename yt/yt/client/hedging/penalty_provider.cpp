#include "penalty_provider.h"

#include "counter.h"
#include "private.h"
#include "public.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <util/generic/hash.h>
#include <util/generic/xrange.h>

namespace NYT::NClient::NHedging::NRpc {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDummyLagProvider
    : public IPenaltyProvider
{
public:
    TDuration Get(const std::string& /*cluster*/) override
    {
        return TDuration::Zero();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLagPenaltyProvider
    : public IPenaltyProvider
{
public:
    TLagPenaltyProvider(
        const TReplicationLagPenaltyProviderOptionsPtr& config,
        NApi::IClientPtr client)
        : Config_(config)
        , Client_(client)
        , Counters_(New<TLagPenaltyProviderCounters>(Config_->TablePath, Config_->ReplicaClusters))
        , Executor_(New<TPeriodicExecutor>(
            NYT::NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TLagPenaltyProvider::UpdateCurrentLagPenalty, MakeWeak(this)),
            Config_->CheckPeriod))
    {
        YT_VERIFY(Executor_);
        YT_VERIFY(Client_);

        for (const auto& cluster : Config_->ReplicaClusters) {
            auto [_, inserted] = ReplicaClusters_.try_emplace(cluster);
            THROW_ERROR_EXCEPTION_UNLESS(inserted, "Duplicate replica cluster %v", cluster);
        }

        GetNodeOptions_.Timeout = TDuration::Seconds(5);
        GetNodeOptions_.ReadFrom = NApi::EMasterChannelKind::Cache;

        Executor_->Start();
    }

    // Checks that all ReplicaIds in ReplicaClusters_ have been set.
    TError CheckAllReplicaIdsPresent() const
    {
        for (const auto& [cluster, info] : ReplicaClusters_) {
            if (!info.ReplicaId) {
                return TError{"ReplicaId was not found for %v", cluster};
            }
        }
        return {};
    }

    // Fills ReplicaIds in ReplicaClusters_.
    void UpdateReplicaIds()
    {
        auto replicasYson = WaitFor(Client_->GetNode(Config_->TablePath + "/@replicas", GetNodeOptions_))
            .ValueOrThrow();
        auto replicasNode = ConvertToNode(replicasYson);

        for (const auto& [key, child] : replicasNode->AsMap()->GetChildren()) {
            auto cluster = child->AsMap()->GetChildOrThrow("cluster_name")->AsString()->GetValue();
            if (const auto contentType = child->AsMap()->FindChild("content_type");
                contentType != nullptr && contentType->AsString()->GetValue() != "data") {
                // skip replication queue from hedging penalty clusters
                continue;
            }
            if (auto* info = ReplicaClusters_.FindPtr(cluster)) {
                info->ReplicaId = NTabletClient::TTableReplicaId::FromString(key);

                const auto replicaLag = TDuration::MilliSeconds(child->AsMap()->GetChildOrThrow("replication_lag_time")->AsInt64()->GetValue());
                const auto newLagPenalty = CalculateLagPenalty(replicaLag);

                info->CurrentLagPenalty.store(newLagPenalty.GetValue(), std::memory_order::relaxed);

                YT_LOG_INFO("Found replica (ReplicaId: %v, Cluster: %v, Table: %v) With Lag %v, Penalty %v",
                    info->ReplicaId,
                    cluster,
                    Config_->TablePath,
                    replicaLag,
                    newLagPenalty);
            };
        }
        CheckAllReplicaIdsPresent()
            .ThrowOnError();
    }

    TDuration CalculateLagPenalty(TDuration replicaLag)
    {
        return replicaLag >= Config_->MaxReplicaLag
            ? Config_->LagPenalty
            : TDuration::Zero();
    }

    void UpdateCurrentLagPenalty()
    {
        try {
            YT_LOG_INFO("Start penalty updater check (Table: %v)",
                Config_->TablePath);

            UpdateReplicaIds();

            Counters_->SuccessRequestCount.Increment();
        } catch (const std::exception& ex) {
            Counters_->ErrorRequestCount.Increment();
            YT_LOG_ERROR(ex, "Failed to update lag penalty (Table: %v)",
                Config_->TablePath);

            if (Config_->ClearPenaltiesOnErrors) {
                for (auto& [cluster, info] : ReplicaClusters_) {
                    info.CurrentLagPenalty.store(0, std::memory_order::relaxed);
                    YT_LOG_INFO("Clear lag penalty for cluster replica (Cluster: %v, Table: %v)",
                        cluster,
                        Config_->TablePath);
                }
            }
        }
    }

    TDuration Get(const std::string& cluster) override
    {
        if (const auto* info = ReplicaClusters_.FindPtr(cluster)) {
            return TDuration::FromValue(info->CurrentLagPenalty.load(std::memory_order::relaxed));
        }
        return TDuration::Zero();
    }

    ~TLagPenaltyProvider()
    {
        YT_UNUSED_FUTURE(Executor_->Stop());
    }

private:
    struct TReplicaInfo
    {
        NTabletClient::TTableReplicaId ReplicaId;
        std::atomic<ui64> CurrentLagPenalty = 0;
    };


    TReplicationLagPenaltyProviderOptionsPtr Config_;

    THashMap<std::string, TReplicaInfo> ReplicaClusters_;
    NApi::IClientPtr Client_;
    TLagPenaltyProviderCountersPtr Counters_;
    NApi::TGetNodeOptions GetNodeOptions_;
    TPeriodicExecutorPtr Executor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPenaltyProviderPtr CreateDummyPenaltyProvider()
{
    return New<TDummyLagProvider>();
}

IPenaltyProviderPtr CreateReplicationLagPenaltyProvider(
    TReplicationLagPenaltyProviderOptionsPtr config,
    NApi::IClientPtr client)
{
    return New<TLagPenaltyProvider>(std::move(config), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
