#include "penalty_provider.h"

#include "counter.h"
#include "private.h"
#include "public.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <util/generic/hash.h>
#include <util/generic/xrange.h>

namespace NYT::NClient::NHedging::NRpc {

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
    TLagPenaltyProvider(const TReplicationLagPenaltyProviderConfig& config, NApi::IClientPtr client)
        : TablePath_(config.GetTablePath())
        , MaxTabletLag_(TDuration::Seconds(config.GetMaxTabletLag()))
        , LagPenalty_(TDuration::MilliSeconds(config.GetLagPenalty()))
        , MaxTabletsWithLagFraction_(config.GetMaxTabletsWithLagFraction())
        , Client_(client)
        , ClearPenaltiesOnErrors_(config.GetClearPenaltiesOnErrors())
        , Counters_(New<TLagPenaltyProviderCounters>(TablePath_,
            TVector<TString>{config.GetReplicaClusters().begin(), config.GetReplicaClusters().end()}))
        , Executor_(New<NConcurrency::TPeriodicExecutor>(
            NYT::NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TLagPenaltyProvider::UpdateCurrentLagPenalty, MakeWeak(this)),
            TDuration::Seconds(config.GetCheckPeriod())))
    {
        YT_VERIFY(Executor_);
        YT_VERIFY(Client_);

        for (const auto& cluster : config.GetReplicaClusters()) {
            auto [_, inserted] = ReplicaClusters_.try_emplace(cluster);
            THROW_ERROR_EXCEPTION_UNLESS(inserted, "Replica cluster %v is listed twice", cluster);
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
        auto replicasNode = NYTree::ConvertToNode(NConcurrency::WaitFor(Client_->GetNode(TablePath_ + "/@replicas", GetNodeOptions_)).ValueOrThrow())->AsMap();

        for (const auto& row : replicasNode->GetChildren()) {
            auto cluster = row.second->AsMap()->GetChildOrThrow("cluster_name")->AsString()->GetValue();
            if (auto* info = ReplicaClusters_.FindPtr(cluster)) {
                info->ReplicaId = NTabletClient::TTableReplicaId::FromString(row.first);
                YT_LOG_INFO("Found replica (ReplicaId: %v, Cluster: %v, Table: %v)",
                    info->ReplicaId,
                    cluster,
                    TablePath_);
            };
        }
        CheckAllReplicaIdsPresent().ThrowOnError();
    }

    ui64 GetTotalNumberOfTablets()
    {
        return NYTree::ConvertTo<ui64>(NConcurrency::WaitFor(Client_->GetNode(TablePath_ + "/@tablet_count", GetNodeOptions_)).ValueOrThrow());
    }

    // Returns a map: ReplicaId -> # of tablets.
    THashMap<NTabletClient::TTableReplicaId, ui64> CalculateNumbersOfTabletsWithLag(const ui64 tabletsCount)
    {
        auto tabletsRange = xrange(tabletsCount);
        auto tabletsInfo = NConcurrency::WaitFor(Client_->GetTabletInfos(TablePath_, {tabletsRange.begin(), tabletsRange.end()})).ValueOrThrow();

        const auto now = TInstant::Now();
        THashMap<NTabletClient::TTableReplicaId, ui64> tabletsWithLag;

        for (const auto& tabletInfo : tabletsInfo) {
            if (!tabletInfo.TableReplicaInfos) {
                continue;
            }

            for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                auto lastReplicationTimestamp = TInstant::Seconds(NTransactionClient::UnixTimeFromTimestamp(replicaInfo.LastReplicationTimestamp));
                if (now - lastReplicationTimestamp > MaxTabletLag_) {
                    ++tabletsWithLag[replicaInfo.ReplicaId];
                }
            }
        }

        return tabletsWithLag;
    }

    TDuration CalculateLagPenalty(const ui64 tabletsCount, const ui64 tabletsWithLag)
    {
        return tabletsWithLag >= tabletsCount * MaxTabletsWithLagFraction_ ? LagPenalty_ : TDuration::Zero();
    }

    void UpdateCurrentLagPenalty()
    {
        try {
            YT_LOG_INFO("Start penalty updater check (Table: %v)",
                TablePath_);

            if (!CheckAllReplicaIdsPresent().IsOK()) {
                UpdateReplicaIds();
            }

            auto tabletsCount = GetTotalNumberOfTablets();
            auto tabletsWithLag = CalculateNumbersOfTabletsWithLag(tabletsCount);

            Counters_->TotalTabletsCount.Update(tabletsCount);

            for (auto& [cluster, info] : ReplicaClusters_) {
                Y_ASSERT(info.ReplicaId);
                auto curTabletsWithLag = tabletsWithLag.Value(info.ReplicaId, 0);
                auto newLagPenalty = CalculateLagPenalty(tabletsCount, curTabletsWithLag);
                info.CurrentLagPenalty.store(newLagPenalty.GetValue(), std::memory_order::relaxed);

                Counters_->LagTabletsCount.at(cluster).Update(curTabletsWithLag);
                YT_LOG_INFO("Finish penalty updater check (Cluster: %v: Table: %v, TabletsWithLag: %v/%v, Penalty: %v)",
                    cluster,
                    TablePath_,
                    curTabletsWithLag,
                    tabletsCount,
                    newLagPenalty);
            }

            Counters_->SuccessRequestCount.Increment();
        } catch (const std::exception& err) {
            Counters_->ErrorRequestCount.Increment();
            YT_LOG_ERROR(err, "Cannot calculate lag (Table: %v)",
                TablePath_);

            if (ClearPenaltiesOnErrors_) {
                for (auto& [cluster, info] : ReplicaClusters_) {
                    info.CurrentLagPenalty.store(0, std::memory_order::relaxed);
                    YT_LOG_INFO("Clearing penalty (Cluster: %v, Table: %v)",
                        cluster,
                        TablePath_);
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

    const TString TablePath_;

    THashMap<std::string, TReplicaInfo> ReplicaClusters_;
    const TDuration MaxTabletLag_;
    const TDuration LagPenalty_;
    const float MaxTabletsWithLagFraction_;
    NApi::IClientPtr Client_;
    const bool ClearPenaltiesOnErrors_;
    TLagPenaltyProviderCountersPtr Counters_;
    NApi::TGetNodeOptions GetNodeOptions_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPenaltyProviderPtr CreateDummyPenaltyProvider()
{
    return New<TDummyLagProvider>();
}

IPenaltyProviderPtr CreateReplicationLagPenaltyProvider(
    TReplicationLagPenaltyProviderConfig config,
    NApi::IClientPtr client)
{
    return New<TLagPenaltyProvider>(std::move(config), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
