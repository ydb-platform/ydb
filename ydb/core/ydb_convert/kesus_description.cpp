#include "kesus_description.h"

#include <ydb/core/protos/kesus.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

namespace NKikimr {

void FillKesusDescription(
    NKikimrSchemeOp::TKesusDescription& out,
    const Ydb::Coordination::Config& inDesc,
    const TString& name)
{
    out.SetName(name);
    out.MutableConfig()->CopyFrom(inDesc);
}

void FillKesusDescription(
    Ydb::Coordination::DescribeNodeResult& out,
    const NKikimrSchemeOp::TKesusDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry)
{
    ConvertDirectoryEntry(inDirEntry, out.mutable_self(), true);
    if (inDesc.HasConfig()) {
        out.mutable_config()->CopyFrom(inDesc.GetConfig());
    }
}

void FillRateLimiterDescription(
    NKikimrKesus::TStreamingQuoterResource& out,
    const Ydb::RateLimiter::Resource& inDesc)
{
    out.SetResourcePath(inDesc.resource_path());
    const auto& inDescProps = inDesc.hierarchical_drr();
    auto& props = *out.MutableHierarchicalDRRResourceConfig();
    props.SetMaxUnitsPerSecond(inDescProps.max_units_per_second());
    props.SetMaxBurstSizeCoefficient(inDescProps.max_burst_size_coefficient());
    props.SetPrefetchCoefficient(inDescProps.prefetch_coefficient());
    props.SetPrefetchWatermark(inDescProps.prefetch_watermark());
    if (inDescProps.has_immediately_fill_up_to()) {
        props.SetImmediatelyFillUpTo(inDescProps.immediately_fill_up_to());
    }
    if (inDesc.has_metering_config()) {
        const auto& inDescAcc = inDesc.metering_config();
        auto& acc = *out.MutableAccountingConfig();
        acc.SetEnabled(inDescAcc.enabled());
        acc.SetReportPeriodMs(inDescAcc.report_period_ms());
        acc.SetAccountPeriodMs(inDescAcc.meter_period_ms());
        acc.SetCollectPeriodSec(inDescAcc.collect_period_sec());
        acc.SetProvisionedUnitsPerSecond(inDescAcc.provisioned_units_per_second());
        acc.SetProvisionedCoefficient(inDescAcc.provisioned_coefficient());
        acc.SetOvershootCoefficient(inDescAcc.overshoot_coefficient());
        auto copyMetric = [] (const Ydb::RateLimiter::MeteringConfig::Metric& inDescMetric, NKikimrKesus::TAccountingConfig::TMetric& metric) {
            metric.SetEnabled(inDescMetric.enabled());
            metric.SetBillingPeriodSec(inDescMetric.billing_period_sec());
            *metric.MutableLabels() = inDescMetric.labels();

            /* overwrite if we have new fields */
            /* TODO: support arbitrary fields in metering core */
            if (inDescMetric.has_metric_fields()) {
                auto& metricFields = inDescMetric.metric_fields().fields();
                if (metricFields.contains("version") && metricFields.at("version").has_string_value()) {
                    metric.SetVersion(metricFields.at("version").string_value());
                }
                if (metricFields.contains("schema") && metricFields.at("schema").has_string_value()) {
                    metric.SetSchema(metricFields.at("schema").string_value());
                }
                if (metricFields.contains("cloud_id") && metricFields.at("cloud_id").has_string_value()) {
                    metric.SetCloudId(metricFields.at("cloud_id").string_value());
                }
                if (metricFields.contains("folder_id") && metricFields.at("folder_id").has_string_value()) {
                    metric.SetFolderId(metricFields.at("folder_id").string_value());
                }
                if (metricFields.contains("resource_id") && metricFields.at("resource_id").has_string_value()) {
                    metric.SetResourceId(metricFields.at("resource_id").string_value());
                }
                if (metricFields.contains("source_id") && metricFields.at("source_id").has_string_value()) {
                    metric.SetSourceId(metricFields.at("source_id").string_value());
                }
                if (metricFields.contains("database") && metricFields.at("database").has_string_value()) {
                    metric.SetDatabase(metricFields.at("database").string_value());
                }
            }
        };
        if (inDescAcc.has_provisioned()) {
            copyMetric(inDescAcc.provisioned(), *acc.MutableProvisioned());
        }
        if (inDescAcc.has_on_demand()) {
            copyMetric(inDescAcc.on_demand(), *acc.MutableOnDemand());
        }
        if (inDescAcc.has_overshoot()) {
            copyMetric(inDescAcc.overshoot(), *acc.MutableOvershoot());
        }
    }
    if (inDescProps.has_replicated_bucket()) {
        const auto& inDescRepl = inDescProps.replicated_bucket();
        auto& repl = *props.MutableReplicatedBucket();
        if (inDescRepl.has_report_interval_ms()) {
            repl.SetReportIntervalMs(inDescRepl.report_interval_ms());
        }
    }
}

void FillRateLimiterDescription(
    Ydb::RateLimiter::Resource& out,
    const NKikimrKesus::TStreamingQuoterResource& inDesc)
{
    out.set_resource_path(inDesc.GetResourcePath());
    const auto& inDescProps = inDesc.GetHierarchicalDRRResourceConfig();
    auto& props = *out.mutable_hierarchical_drr();
    props.set_max_units_per_second(inDescProps.GetMaxUnitsPerSecond());
    props.set_max_burst_size_coefficient(inDescProps.GetMaxBurstSizeCoefficient());
    props.set_prefetch_coefficient(inDescProps.GetPrefetchCoefficient());
    props.set_prefetch_watermark(inDescProps.GetPrefetchWatermark());
    if (inDescProps.HasImmediatelyFillUpTo()) {
        props.set_immediately_fill_up_to(inDescProps.GetImmediatelyFillUpTo());
    }
    if (inDesc.HasAccountingConfig()) {
        const auto& inDescAcc = inDesc.GetAccountingConfig();
        auto& acc = *out.mutable_metering_config();
        acc.set_enabled(inDescAcc.GetEnabled());
        acc.set_report_period_ms(inDescAcc.GetReportPeriodMs());
        acc.set_meter_period_ms(inDescAcc.GetAccountPeriodMs());
        acc.set_collect_period_sec(inDescAcc.GetCollectPeriodSec());
        acc.set_provisioned_units_per_second(inDescAcc.GetProvisionedUnitsPerSecond());
        acc.set_provisioned_coefficient(inDescAcc.GetProvisionedCoefficient());
        acc.set_overshoot_coefficient(inDescAcc.GetOvershootCoefficient());
        auto copyMetric = [] (const NKikimrKesus::TAccountingConfig::TMetric& inDescMetric, Ydb::RateLimiter::MeteringConfig::Metric& metric) {
            metric.set_enabled(inDescMetric.GetEnabled());
            metric.set_billing_period_sec(inDescMetric.GetBillingPeriodSec());
            *metric.mutable_labels() = inDescMetric.GetLabels();

            /* TODO: support arbitrary fields in metering core */
            auto& metricFields = *metric.mutable_metric_fields()->mutable_fields();
            metricFields["version"].set_string_value(inDescMetric.GetVersion());
            metricFields["schema"].set_string_value(inDescMetric.GetSchema());
            metricFields["cloud_id"].set_string_value(inDescMetric.GetCloudId());
            metricFields["folder_id"].set_string_value(inDescMetric.GetFolderId());
            metricFields["resource_id"].set_string_value(inDescMetric.GetResourceId());
            metricFields["source_id"].set_string_value(inDescMetric.GetSourceId());
            metricFields["database"].set_string_value(inDescMetric.GetDatabase());
        };
        if (inDescAcc.HasProvisioned()) {
            copyMetric(inDescAcc.GetProvisioned(), *acc.mutable_provisioned());
        }
        if (inDescAcc.HasOnDemand()) {
            copyMetric(inDescAcc.GetOnDemand(), *acc.mutable_on_demand());
        }
        if (inDescAcc.HasOvershoot()) {
            copyMetric(inDescAcc.GetOvershoot(), *acc.mutable_overshoot());
        }
    }
    if (inDescProps.HasReplicatedBucket()) {
        const auto& inDescRepl = inDescProps.GetReplicatedBucket();
        auto& repl = *props.mutable_replicated_bucket();
        if (inDescRepl.HasReportIntervalMs()) {
            repl.set_report_interval_ms(inDescRepl.GetReportIntervalMs());
        }
    }
}

} // namespace NKikimr
