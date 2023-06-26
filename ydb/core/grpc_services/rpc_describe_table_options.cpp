#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include "service_table.h"

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/ydb_convert/table_profiles.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NSchemeShard;
using namespace NActors;
using namespace NConsole;
using namespace Ydb;
using namespace Ydb::Table;

using TEvDescribeTableOptionsRequest = TGrpcRequestOperationCall<Ydb::Table::DescribeTableOptionsRequest,
    Ydb::Table::DescribeTableOptionsResponse>;

class TDescribeTableOptionsRPC : public TRpcSchemeRequestActor<TDescribeTableOptionsRPC, TEvDescribeTableOptionsRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeTableOptionsRPC, TEvDescribeTableOptionsRequest>;

public:
    TDescribeTableOptionsRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendConfigRequest(ctx);
        ctx.Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup(WakeupTagGetConfig));
        Become(&TDescribeTableOptionsRPC::StateGetConfig);
    }

private:
    void StateGetConfig(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConfigsDispatcher::TEvGetConfigResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            default: TBase::StateFuncBase(ev);
        }
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateWork(ev);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &/*ev*/, const TActorContext &ctx)
    {
        LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY,
                   "TDescribeTableOptionsRPC: cannot deliver config request to Configs Dispatcher");
         NYql::TIssues issues;
         issues.AddIssue(NYql::TIssue("Cannot get table profiles (service unavailable)"));
         Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);
    }

    void Handle(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr &ev, const TActorContext &ctx) {
        auto &config = ev->Get()->Config->GetTableProfilesConfig();
        Profiles.Load(config);

        Ydb::Table::DescribeTableOptionsResult result;

        for (auto &pr: Profiles.CompactionPolicies) {
            auto &description = *result.add_compaction_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            if (pr.second.HasCompactionPolicy())
                labels["generations"] = ToString(pr.second.GetCompactionPolicy().GenerationSize());
        }

        for (auto &pr: Profiles.ExecutionPolicies) {
            auto &description = *result.add_execution_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            labels["out_of_order"] = pr.second.GetPipelineConfig().GetEnableOutOfOrder() ? "enabled" : "disabled";
            if (pr.second.GetPipelineConfig().GetEnableOutOfOrder())
                labels["pipeline_width"] = ToString(pr.second.GetPipelineConfig().GetNumActiveTx());
            labels["immediate_tx"] = pr.second.GetPipelineConfig().GetDisableImmediate() ? "disabled" : "enabled";
            labels["bloom_filter"] = pr.second.GetEnableFilterByKey() ? "enabled" : "disabled";
            if (pr.second.HasTxReadSizeLimit())
                labels["tx_read_limit"] = ToString(pr.second.GetTxReadSizeLimit());
        }

        for (auto &pr: Profiles.PartitioningPolicies) {
            auto &description = *result.add_partitioning_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            if (pr.second.GetUniformPartitionsCount())
                labels["uniform_parts"] = ToString(pr.second.GetUniformPartitionsCount());
            labels["auto_split"] = pr.second.GetAutoSplit() ? "enabled" : "disabled";
            labels["auto_merge"] = pr.second.GetAutoMerge() ? "enabled" : "disabled";
            if (pr.second.GetAutoSplit() && pr.second.HasSizeToSplit())
                labels["split_threshold"] = ToString(pr.second.GetSizeToSplit());
        }

        for (auto &pr: Profiles.StoragePolicies) {
            auto &description = *result.add_storage_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            for (size_t i = 0; i < pr.second.ColumnFamiliesSize(); ++i) {
                auto &family = pr.second.GetColumnFamilies(i);
                if (family.GetId() == 0) {
                    if (family.GetColumnCache() == NKikimrSchemeOp::ColumnCacheEver)
                        labels["in_memory"] = "true";
                    else
                        labels["in_memory"] = "false";
                    if (family.GetStorageConfig().HasSysLog())
                        labels["syslog"] = family.GetStorageConfig().GetSysLog().GetPreferredPoolKind();
                    if (family.GetStorageConfig().HasLog())
                        labels["log"] = family.GetStorageConfig().GetLog().GetPreferredPoolKind();
                    if (family.GetStorageConfig().HasData())
                        labels["data"] = family.GetStorageConfig().GetData().GetPreferredPoolKind();
                    if (family.GetStorageConfig().HasExternal())
                        labels["external"] = family.GetStorageConfig().GetExternal().GetPreferredPoolKind();
                    if (family.GetStorageConfig().GetDataThreshold())
                        labels["medium_threshold"] = ToString(family.GetStorageConfig().GetDataThreshold());
                    if (family.GetStorageConfig().GetExternalThreshold())
                        labels["external_threshold"] = ToString(family.GetStorageConfig().GetExternalThreshold());
                    if (family.GetColumnCodec() == NKikimrSchemeOp::ColumnCodecLZ4)
                        labels["codec"] = "lz4";
                    else
                        labels["codec"] = "none";
                    break;
                }
            }
        }

        for (auto &pr: Profiles.ReplicationPolicies) {
            auto &description = *result.add_replication_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            if (pr.second.GetFollowerCount()) {
                labels["followers"] = ToString(pr.second.GetFollowerCount());
                labels["promotion"] = pr.second.GetAllowFollowerPromotion() ? "enabled" : "disabled";
                labels["per_zone"] = pr.second.GetCrossDataCenter() ? "true" : "false";
            } else {
                labels["followers"] = "disabled";
            }
        }

        for (auto &pr: Profiles.CachingPolicies) {
            auto &description = *result.add_caching_policy_presets();
            description.set_name(pr.first);
            auto &labels = *description.mutable_labels();
            if (pr.second.GetExecutorCacheSize())
                labels["executor_cache"] = ToString(pr.second.GetExecutorCacheSize());
        }

        for (auto &pr: Profiles.TableProfiles) {
            auto &description = *result.add_table_profile_presets();
            description.set_name(pr.first);
            description.set_default_storage_policy(pr.second.GetStoragePolicy());
            for (auto &pr : Profiles.StoragePolicies)
                description.add_allowed_storage_policies(pr.first);
            description.set_default_compaction_policy(pr.second.GetCompactionPolicy());
            for (auto &pr : Profiles.CompactionPolicies)
                description.add_allowed_compaction_policies(pr.first);
            description.set_default_partitioning_policy(pr.second.GetPartitioningPolicy());
            for (auto &pr : Profiles.PartitioningPolicies)
                description.add_allowed_partitioning_policies(pr.first);
            description.set_default_execution_policy(pr.second.GetExecutionPolicy());
            for (auto &pr : Profiles.ExecutionPolicies)
                description.add_allowed_execution_policies(pr.first);
            description.set_default_replication_policy(pr.second.GetReplicationPolicy());
            for (auto &pr : Profiles.ReplicationPolicies)
                description.add_allowed_replication_policies(pr.first);
            description.set_default_caching_policy(pr.second.GetCachingPolicy());
            for (auto &pr : Profiles.CachingPolicies)
                description.add_allowed_caching_policies(pr.first);
        }

        this->Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
            case WakeupTagGetConfig: {
                LOG_CRIT_S(ctx, NKikimrServices::GRPC_PROXY,
                   "TDescribeTableOptionsRPC: cannot get table profiles (timeout)");
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue("Tables profiles config not available."));
                return Reply(Ydb::StatusIds::UNAVAILABLE, issues, ctx);
            }

            default:
                TBase::HandleWakeup(ev, ctx);
        }
    }

    void SendConfigRequest(const TActorContext &ctx) {
        ui32 configKind = (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem;
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
                 new TEvConfigsDispatcher::TEvGetConfigRequest(configKind),
                 IEventHandle::FlagTrackDelivery);
    }

private:
    TTableProfiles Profiles;
};

void DoDescribeTableOptionsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDescribeTableOptionsRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
