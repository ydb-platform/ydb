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
            if (auto* info = ReplicaClusters_.FindPtr(cluster)) {
                info->ReplicaId = NTabletClient::TTableReplicaId::FromString(key);
                YT_LOG_INFO("Found replica (ReplicaId: %v, Cluster: %v, Table: %v)",
                    info->ReplicaId,
                    cluster,
                    Config_->TablePath);
            };
        }
        CheckAllReplicaIdsPresent()
            .ThrowOnError();
    }

    int GetTotalNumberOfTablets()
    {
        auto tabletCountNode = WaitFor(Client_->GetNode(Config_->TablePath + "/@tablet_count", GetNodeOptions_))
            .ValueOrThrow();
        return ConvertTo<int>(tabletCountNode);
    }

    // Returns a map: ReplicaId -> # of tablets.
    THashMap<NTabletClient::TTableReplicaId, ui64> CalculateTabletWithLagCounts(int tabletCount)
    {
        auto tabletsRange = xrange(tabletCount);
        auto tabletsInfo = WaitFor(Client_->GetTabletInfos(Config_->TablePath, {tabletsRange.begin(), tabletsRange.end()}))
            .ValueOrThrow();

        const auto now = TInstant::Now();
        THashMap<NTabletClient::TTableReplicaId, ui64> tabletsWithLag;

        for (const auto& tabletInfo : tabletsInfo) {
            if (!tabletInfo.TableReplicaInfos) {
                continue;
            }

            for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                auto lastReplicationTimestamp = TInstant::Seconds(NTransactionClient::UnixTimeFromTimestamp(replicaInfo.LastReplicationTimestamp));
                if (now - lastReplicationTimestamp > Config_->MaxTabletLag) {
                    ++tabletsWithLag[replicaInfo.ReplicaId];
                }
            }
        }

        return tabletsWithLag;
    }

    TDuration CalculateLagPenalty(int tabletCount, int tabletWithLagCount)
    {
        return tabletWithLagCount >= Config_->MaxTabletsWithLagFraction * tabletCount
            ? Config_->LagPenalty
            : TDuration::Zero();
    }

    void UpdateCurrentLagPenalty()
    {
        try {
            YT_LOG_INFO("Start penalty updater check (Table: %v)",
                Config_->TablePath);

            if (!CheckAllReplicaIdsPresent().IsOK()) {
                UpdateReplicaIds();
            }

            auto tabletCount = GetTotalNumberOfTablets();
            auto tabletWithLagCountPerReplica = CalculateTabletWithLagCounts(tabletCount);

            Counters_->TotalTabletCount.Update(tabletCount);

            for (auto& [cluster, info] : ReplicaClusters_) {
                YT_ASSERT(info.ReplicaId);
                auto tabletWithLagCount = tabletWithLagCountPerReplica.Value(info.ReplicaId, 0);
                auto newLagPenalty = CalculateLagPenalty(tabletCount, tabletWithLagCount);
                info.CurrentLagPenalty.store(newLagPenalty.GetValue(), std::memory_order::relaxed);

                GetOrCrash(Counters_->TabletWithLagCountPerReplica, cluster).Update(tabletWithLagCount);
                YT_LOG_INFO("Lag penalty for cluster replica updated (Cluster: %v, Table: %v, TabletWithLagCount: %v/%v, Penalty: %v)",
                    cluster,
                    Config_->TablePath,
                    tabletWithLagCount,
                    tabletCount,
                    newLagPenalty);
            }

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
