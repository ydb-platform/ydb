#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_rate_limiter_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <google/protobuf/util/json_util.h>

namespace NYdb::inline Dev::NRateLimiter {

TReplicatedBucketSettings::TReplicatedBucketSettings(const Ydb::RateLimiter::ReplicatedBucketSettings& proto) {
    if (proto.has_report_interval_ms()) {
        ReportInterval_ = std::chrono::milliseconds(proto.report_interval_ms());
    }
}

void TReplicatedBucketSettings::SerializeTo(Ydb::RateLimiter::ReplicatedBucketSettings& proto) const {
    if (ReportInterval_) {
        proto.set_report_interval_ms(ReportInterval_->count());
    }
}

TLeafBehavior::EBehavior TLeafBehavior::GetBehavior() const {
    return static_cast<EBehavior>(BehaviorSettings_.index());
}

TLeafBehavior::TLeafBehavior(const TReplicatedBucketSettings& replicatedBucket)
    : BehaviorSettings_(replicatedBucket)
{
}

TLeafBehavior::TLeafBehavior(const Ydb::RateLimiter::ReplicatedBucketSettings& replicatedBucket)
    : BehaviorSettings_(replicatedBucket)
{
}

const TReplicatedBucketSettings& TLeafBehavior::GetReplicatedBucket() const {
    return std::get<TReplicatedBucketSettings>(BehaviorSettings_);
}

void TLeafBehavior::SerializeTo(Ydb::RateLimiter::HierarchicalDrrSettings& proto) const {
    switch (GetBehavior()) {
        case REPLICATED_BUCKET:
            return GetReplicatedBucket().SerializeTo(*proto.mutable_replicated_bucket());
    }
}

template <class TDerived>
THierarchicalDrrSettings<TDerived>::THierarchicalDrrSettings(const Ydb::RateLimiter::HierarchicalDrrSettings& proto) {
    if (proto.max_units_per_second()) {
        MaxUnitsPerSecond_ = proto.max_units_per_second();
    }

    if (proto.max_burst_size_coefficient()) {
        MaxBurstSizeCoefficient_ = proto.max_burst_size_coefficient();
    }

    if (proto.prefetch_coefficient()) {
        PrefetchCoefficient_ = proto.prefetch_coefficient();
    }

    if (proto.prefetch_watermark()) {
        PrefetchWatermark_ = proto.prefetch_watermark();
    }

    if (proto.has_immediately_fill_up_to()) {
        ImmediatelyFillUpTo_ = proto.immediately_fill_up_to();
    }

    switch (proto.leaf_behavior_case()) {
        case Ydb::RateLimiter::HierarchicalDrrSettings::kReplicatedBucket:
            LeafBehavior_.emplace(proto.replicated_bucket());
            break;
        case Ydb::RateLimiter::HierarchicalDrrSettings::LEAF_BEHAVIOR_NOT_SET:
            break;
    }
}

template <class TDerived>
void THierarchicalDrrSettings<TDerived>::SerializeTo(Ydb::RateLimiter::HierarchicalDrrSettings& proto) const {
    if (MaxUnitsPerSecond_) {
        proto.set_max_units_per_second(*MaxUnitsPerSecond_);
    }

    if (MaxBurstSizeCoefficient_) {
        proto.set_max_burst_size_coefficient(*MaxBurstSizeCoefficient_);
    }

    if (PrefetchCoefficient_) {
        proto.set_prefetch_coefficient(*PrefetchCoefficient_);
    }

    if (PrefetchWatermark_) {
        proto.set_prefetch_watermark(*PrefetchWatermark_);
    }

    if (ImmediatelyFillUpTo_) {
        proto.set_immediately_fill_up_to(*ImmediatelyFillUpTo_);
    }

    if (LeafBehavior_) {
        LeafBehavior_->SerializeTo(proto);
    }
}

TMetric::TMetric(const Ydb::RateLimiter::MeteringConfig_Metric& proto) {
    Enabled_ = proto.enabled();
    if (proto.billing_period_sec()) {
        BillingPeriod_ = std::chrono::seconds(proto.billing_period_sec());
    }
    for (const auto& [k, v] : proto.labels()) {
        Labels_[k] = v;
    }
    if (proto.has_metric_fields()) {
        TStringType jsonStr;
        if (auto st = google::protobuf::util::MessageToJsonString(proto.metric_fields(), &jsonStr); st.ok()) {
            MetricFieldsJson_ = jsonStr;
        }
    }
}

void TMetric::SerializeTo(Ydb::RateLimiter::MeteringConfig_Metric& proto) const {
    proto.set_enabled(Enabled_);
    if (BillingPeriod_) {
        proto.set_billing_period_sec(BillingPeriod_->count());
    }
    for (const auto& [k, v] : Labels_) {
        (*proto.mutable_labels())[k] = v;
    }
    if (!MetricFieldsJson_.empty()) {
        google::protobuf::util::JsonStringToMessage(MetricFieldsJson_, proto.mutable_metric_fields());
    }
}

TMeteringConfig::TMeteringConfig(const Ydb::RateLimiter::MeteringConfig& proto) {
    Enabled_ = proto.enabled();
    if (proto.report_period_ms()) {
        ReportPeriod_ = std::chrono::milliseconds(proto.report_period_ms());
    }
    if (proto.meter_period_ms()) {
        MeterPeriod_ = std::chrono::milliseconds(proto.meter_period_ms());
    }
    if (proto.collect_period_sec()) {
        CollectPeriod_ = std::chrono::seconds(proto.collect_period_sec());
    }
    if (proto.provisioned_units_per_second()) {
        ProvisionedUnitsPerSecond_ = proto.provisioned_units_per_second();
    }
    if (proto.provisioned_coefficient()) {
        ProvisionedCoefficient_ = proto.provisioned_coefficient();
    }
    if (proto.overshoot_coefficient()) {
        OvershootCoefficient_ = proto.overshoot_coefficient();
    }
    if (proto.has_provisioned()) {
        Provisioned_.emplace(proto.provisioned());
    }
    if (proto.has_on_demand()) {
        OnDemand_.emplace(proto.on_demand());
    }
    if (proto.has_overshoot()) {
        Overshoot_.emplace(proto.overshoot());
    }
}

void TMeteringConfig::SerializeTo(Ydb::RateLimiter::MeteringConfig& proto) const {
    proto.set_enabled(Enabled_);
    if (ReportPeriod_) {
        proto.set_report_period_ms(ReportPeriod_->count());
    }
    if (MeterPeriod_) {
        proto.set_meter_period_ms(MeterPeriod_->count());
    }
    if (CollectPeriod_) {
        proto.set_collect_period_sec(CollectPeriod_->count());
    }
    if (ProvisionedUnitsPerSecond_) {
        proto.set_provisioned_units_per_second(*ProvisionedUnitsPerSecond_);
    }
    if (ProvisionedCoefficient_) {
        proto.set_provisioned_coefficient(*ProvisionedCoefficient_);
    }
    if (OvershootCoefficient_) {
        proto.set_overshoot_coefficient(*OvershootCoefficient_);
    }
    if (Provisioned_) {
        Provisioned_->SerializeTo(*proto.mutable_provisioned());
    }
    if (OnDemand_) {
        OnDemand_->SerializeTo(*proto.mutable_on_demand());
    }
    if (Overshoot_) {
        Overshoot_->SerializeTo(*proto.mutable_overshoot());
    }
}

template struct THierarchicalDrrSettings<TCreateResourceSettings>;
template struct THierarchicalDrrSettings<TAlterResourceSettings>;
template struct THierarchicalDrrSettings<TDescribeResourceResult::THierarchicalDrrProps>;

TCreateResourceSettings::TCreateResourceSettings(const Ydb::RateLimiter::CreateResourceRequest& proto)
    : THierarchicalDrrSettings(proto.resource().hierarchical_drr())
{
    if (proto.resource().has_metering_config()) {
        MeteringConfig_ = proto.resource().metering_config();
    }
}

TListResourcesResult::TListResourcesResult(TStatus status, std::vector<std::string> paths)
    : TStatus(std::move(status))
    , ResourcePaths_(std::move(paths))
{
}

TDescribeResourceResult::TDescribeResourceResult(TStatus status, const Ydb::RateLimiter::DescribeResourceResult& result)
    : TStatus(std::move(status))
    , ResourcePath_(result.resource().resource_path())
    , HierarchicalDrrProps_(result.resource().hierarchical_drr())
{
    if (result.resource().has_metering_config()) {
        MeteringConfig_ = result.resource().metering_config();
    }
}

TDescribeResourceResult::THierarchicalDrrProps::THierarchicalDrrProps(const Ydb::RateLimiter::HierarchicalDrrSettings& settings)
    : THierarchicalDrrSettings<THierarchicalDrrProps>(settings)
{
}

class TRateLimiterClient::TImpl : public TClientImplCommon<TRateLimiterClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    template <class TRequest, class TSettings>
    static TRequest MakePropsCreateOrAlterRequest(const std::string& coordinationNodePath, const std::string& resourcePath, const TSettings& settings) {
        TRequest request = MakeOperationRequest<TRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});

        Ydb::RateLimiter::Resource& resource = *request.mutable_resource();
        resource.set_resource_path(TStringType{resourcePath});

        Ydb::RateLimiter::HierarchicalDrrSettings& hdrr = *resource.mutable_hierarchical_drr();
        if (settings.MaxUnitsPerSecond_) {
            hdrr.set_max_units_per_second(*settings.MaxUnitsPerSecond_);
        }
        if (settings.MaxBurstSizeCoefficient_) {
            hdrr.set_max_burst_size_coefficient(*settings.MaxBurstSizeCoefficient_);
        }
        if (settings.PrefetchCoefficient_) {
            hdrr.set_prefetch_coefficient(*settings.PrefetchCoefficient_);
        }
        if (settings.PrefetchWatermark_) {
            hdrr.set_prefetch_watermark(*settings.PrefetchWatermark_);
        }
        if (settings.ImmediatelyFillUpTo_) {
            hdrr.set_immediately_fill_up_to(*settings.ImmediatelyFillUpTo_);
        }
        if (settings.LeafBehavior_) {
            settings.LeafBehavior_->SerializeTo(hdrr);
        }
        if (settings.MeteringConfig_) {
            settings.MeteringConfig_->SerializeTo(*resource.mutable_metering_config());
        }

        return request;
    }

    TAsyncStatus CreateResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TCreateResourceSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::RateLimiter::CreateResourceRequest>(coordinationNodePath, resourcePath, settings);

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::CreateResourceRequest, Ydb::RateLimiter::CreateResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncCreateResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAlterResourceSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::RateLimiter::AlterResourceRequest>(coordinationNodePath, resourcePath, settings);

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::AlterResourceRequest, Ydb::RateLimiter::AlterResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncAlterResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus DropResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDropResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::DropResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::DropResourceRequest, Ydb::RateLimiter::DropResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncDropResource,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncListResourcesResult ListResources(const std::string& coordinationNodePath, const std::string& resourcePath, const TListResourcesSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::ListResourcesRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});
        request.set_recursive(settings.Recursive_);

        auto promise = NThreading::NewPromise<TListResourcesResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<std::string> list;
                if (any) {
                    Ydb::RateLimiter::ListResourcesResult result;
                    any->UnpackTo(&result);
                    list.reserve(result.resource_paths_size());
                    for (const std::string& path : result.resource_paths()) {
                        list.push_back(path);
                    }
                }

                TListResourcesResult val(TStatus(std::move(status)), std::move(list));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::ListResourcesRequest, Ydb::RateLimiter::ListResourcesResponse>(
            std::move(request),
            extractor,
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncListResources,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeResourceResult DescribeResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDescribeResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::DescribeResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        auto promise = NThreading::NewPromise<TDescribeResourceResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::RateLimiter::DescribeResourceResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeResourceResult val(TStatus(std::move(status)), result);
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::DescribeResourceRequest, Ydb::RateLimiter::DescribeResourceResponse>(
            std::move(request),
            extractor,
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncDescribeResource,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus AcquireResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAcquireResourceSettings& settings) {
        auto request = MakeOperationRequest<Ydb::RateLimiter::AcquireResourceRequest>(settings);
        request.set_coordination_node_path(TStringType{coordinationNodePath});
        request.set_resource_path(TStringType{resourcePath});

        if (settings.IsUsedAmount_) {
            request.set_used(settings.Amount_.value());
        } else {
            request.set_required(settings.Amount_.value());
        }

        return RunSimple<Ydb::RateLimiter::V1::RateLimiterService, Ydb::RateLimiter::AcquireResourceRequest, Ydb::RateLimiter::AcquireResourceResponse>(
            std::move(request),
            &Ydb::RateLimiter::V1::RateLimiterService::Stub::AsyncAcquireResource,
            TRpcRequestSettings::Make(settings));
    }
};

TRateLimiterClient::TRateLimiterClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncStatus TRateLimiterClient::CreateResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TCreateResourceSettings& settings) {
    return Impl_->CreateResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::AlterResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAlterResourceSettings& settings) {
    return Impl_->AlterResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::DropResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDropResourceSettings& settings) {
    return Impl_->DropResource(coordinationNodePath, resourcePath, settings);
}

TAsyncListResourcesResult TRateLimiterClient::ListResources(const std::string& coordinationNodePath, const std::string& resourcePath, const TListResourcesSettings& settings) {
    return Impl_->ListResources(coordinationNodePath, resourcePath, settings);
}

TAsyncDescribeResourceResult TRateLimiterClient::DescribeResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDescribeResourceSettings& settings) {
    return Impl_->DescribeResource(coordinationNodePath, resourcePath, settings);
}

TAsyncStatus TRateLimiterClient::AcquireResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAcquireResourceSettings& settings) {
    return Impl_->AcquireResource(coordinationNodePath, resourcePath, settings);
}

} // namespace NYdb::NRateLimiter
