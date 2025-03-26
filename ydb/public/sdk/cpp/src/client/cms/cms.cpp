#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb::inline Dev::NCms {

namespace {
    EState ConvertState(Ydb::Cms::GetDatabaseStatusResult_State protoState) {
        switch (protoState) {
            case Ydb::Cms::GetDatabaseStatusResult_State_STATE_UNSPECIFIED:
                return EState::StateUnspecified;
            case Ydb::Cms::GetDatabaseStatusResult_State_CREATING:
                return EState::Creating;
            case Ydb::Cms::GetDatabaseStatusResult_State_RUNNING:
                return EState::Running;
            case Ydb::Cms::GetDatabaseStatusResult_State_REMOVING:
                return EState::Removing;
            case Ydb::Cms::GetDatabaseStatusResult_State_PENDING_RESOURCES:
                return EState::PendingResources;
            case Ydb::Cms::GetDatabaseStatusResult_State_CONFIGURING:
                return EState::Configuring;
            default:
                return EState::StateUnspecified;
        }
    }

    void SerializeToImpl(
        const TResourcesKind& resourcesKind,
        const TSchemaOperationQuotas& schemaQuotas,
        const TDatabaseQuotas& dbQuotas,
        const TScaleRecommenderPolicies& scaleRecommenderPolicies,
        Ydb::Cms::CreateDatabaseRequest& out)
    {
        if (std::holds_alternative<NCms::TResources>(resourcesKind)) {
            const auto& resources = std::get<NCms::TResources>(resourcesKind);
            for (const auto& storageUnit : resources.StorageUnits) {
                auto* protoUnit = out.mutable_resources()->add_storage_units();
                protoUnit->set_unit_kind(storageUnit.UnitKind);
                protoUnit->set_count(storageUnit.Count);
            }
            for (const auto& computationalUnit : resources.ComputationalUnits) {
                auto* protoUnit = out.mutable_resources()->add_computational_units();
                protoUnit->set_unit_kind(computationalUnit.UnitKind);
                protoUnit->set_count(computationalUnit.Count);
                protoUnit->set_availability_zone(computationalUnit.AvailabilityZone);
            }
        } else if (std::holds_alternative<NCms::TSharedResources>(resourcesKind)) {
            const auto& resources = std::get<NCms::TSharedResources>(resourcesKind);
            for (const auto& storageUnit : resources.StorageUnits) {
                auto* protoUnit = out.mutable_shared_resources()->add_storage_units();
                protoUnit->set_unit_kind(storageUnit.UnitKind);
                protoUnit->set_count(storageUnit.Count);
            }
            for (const auto& computationalUnit : resources.ComputationalUnits) {
                auto* protoUnit = out.mutable_shared_resources()->add_computational_units();
                protoUnit->set_unit_kind(computationalUnit.UnitKind);
                protoUnit->set_count(computationalUnit.Count);
                protoUnit->set_availability_zone(computationalUnit.AvailabilityZone);
            }
        } else if (std::holds_alternative<NCms::TServerlessResources>(resourcesKind)) {
            const auto& resources = std::get<NCms::TServerlessResources>(resourcesKind);
            out.mutable_serverless_resources()->set_shared_database_path(resources.SharedDatabasePath);
        } else if (std::holds_alternative<std::monostate>(resourcesKind)) {
            out.clear_resources_kind();
        }
        
        for (const auto& quota : schemaQuotas.LeakyBucketQuotas) {
            auto protoQuota = out.mutable_schema_operation_quotas()->add_leaky_bucket_quotas();
            protoQuota->set_bucket_seconds(quota.BucketSeconds);
            protoQuota->set_bucket_size(quota.BucketSize);
        }
    
        out.mutable_database_quotas()->set_data_size_hard_quota(dbQuotas.DataSizeHardQuota);
        out.mutable_database_quotas()->set_data_size_soft_quota(dbQuotas.DataSizeSoftQuota);
        out.mutable_database_quotas()->set_data_stream_shards_quota(dbQuotas.DataStreamShardsQuota);
        out.mutable_database_quotas()->set_data_stream_reserved_storage_quota(dbQuotas.DataStreamReservedStorageQuota);
        out.mutable_database_quotas()->set_ttl_min_run_internal_seconds(dbQuotas.TtlMinRunInternalSeconds);
    
        for (const auto& quota : dbQuotas.StorageQuotas) {
            auto protoQuota = out.mutable_database_quotas()->add_storage_quotas();
            protoQuota->set_unit_kind(quota.UnitKind);
            protoQuota->set_data_size_hard_quota(quota.DataSizeHardQuota);
            protoQuota->set_data_size_soft_quota(quota.DataSizeSoftQuota);
        }
    
        for (const auto& policy : scaleRecommenderPolicies.Policies) {
            auto* protoPolicy = out.mutable_scale_recommender_policies()->add_policies();
            if (std::holds_alternative<NCms::TTargetTrackingPolicy>(policy.Policy)) {
                const auto& targetTracking = std::get<NCms::TTargetTrackingPolicy>(policy.Policy);
                auto* protoTargetTracking = protoPolicy->mutable_target_tracking_policy();
                if (std::holds_alternative<NCms::TTargetTrackingPolicy::TAverageCpuUtilizationPercent>(targetTracking.Target)) {
                    const auto& target = std::get<NCms::TTargetTrackingPolicy::TAverageCpuUtilizationPercent>(targetTracking.Target);
                    protoTargetTracking->set_average_cpu_utilization_percent(target);
                } else if (std::holds_alternative<std::monostate>(targetTracking.Target)) {
                    protoTargetTracking->clear_target();
                }
            } else if (std::holds_alternative<std::monostate>(policy.Policy)) {
                protoPolicy->clear_policy();
            }
        }
    }
} // anonymous namespace

TListDatabasesResult::TListDatabasesResult(TStatus&& status, const Ydb::Cms::ListDatabasesResult& proto)
    : TStatus(std::move(status))
    , Paths_(proto.paths().begin(), proto.paths().end())
{}

const std::vector<std::string>& TListDatabasesResult::GetPaths() const {
    return Paths_;
}

TStorageUnits::TStorageUnits(const Ydb::Cms::StorageUnits& proto)
    : UnitKind(proto.unit_kind())
    , Count(proto.count())
{}

TComputationalUnits::TComputationalUnits(const Ydb::Cms::ComputationalUnits& proto) 
    : UnitKind(proto.unit_kind())
    , AvailabilityZone(proto.availability_zone())
    , Count(proto.count())
{}

TAllocatedComputationalUnit::TAllocatedComputationalUnit(const Ydb::Cms::AllocatedComputationalUnit& proto)
    : Host(proto.host())
    , Port(proto.port())
    , UnitKind(proto.unit_kind())
{}

TResources::TResources(const Ydb::Cms::Resources& proto)
    : StorageUnits(proto.storage_units().begin(), proto.storage_units().end())
    , ComputationalUnits(proto.computational_units().begin(), proto.computational_units().end())
{}

TServerlessResources::TServerlessResources(const Ydb::Cms::ServerlessResources& proto)
    : SharedDatabasePath(proto.shared_database_path())
{}

TSchemaOperationQuotas::TLeakyBucket::TLeakyBucket(const Ydb::Cms::SchemaOperationQuotas_LeakyBucket& proto)
    : BucketSize(proto.bucket_size())
    , BucketSeconds(proto.bucket_seconds())
{}

TSchemaOperationQuotas::TSchemaOperationQuotas(const Ydb::Cms::SchemaOperationQuotas& proto)
    : LeakyBucketQuotas(proto.leaky_bucket_quotas().begin(), proto.leaky_bucket_quotas().end())
{}

TDatabaseQuotas::TStorageQuotas::TStorageQuotas(const Ydb::Cms::DatabaseQuotas_StorageQuotas& proto)
    : UnitKind(proto.unit_kind()) 
    , DataSizeHardQuota(proto.data_size_hard_quota())
    , DataSizeSoftQuota(proto.data_size_soft_quota())
{}

TDatabaseQuotas::TDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& proto)
    : DataSizeHardQuota(proto.data_size_hard_quota())
    , DataSizeSoftQuota(proto.data_size_soft_quota())
    , DataStreamShardsQuota(proto.data_stream_shards_quota())
    , DataStreamReservedStorageQuota(proto.data_stream_reserved_storage_quota())
    , TtlMinRunInternalSeconds(proto.ttl_min_run_internal_seconds())
    , StorageQuotas(proto.storage_quotas().begin(), proto.storage_quotas().end())
{}

TTargetTrackingPolicy::TTargetTrackingPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy& proto)
{
    switch (proto.target_case()) {
        case Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy::kAverageCpuUtilizationPercent:
            Target = proto.average_cpu_utilization_percent();
            break;
        case Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy::TARGET_NOT_SET:
            Target = std::monostate();
            break;
    }
}

TScaleRecommenderPolicy::TScaleRecommenderPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy& proto)
{
    switch (proto.policy_case()) {
        case Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy::kTargetTrackingPolicy:
            Policy = proto.target_tracking_policy();
            break;
        case Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy::POLICY_NOT_SET:
            Policy = std::monostate();
            break;
    }
}

TScaleRecommenderPolicies::TScaleRecommenderPolicies(const Ydb::Cms::ScaleRecommenderPolicies& proto) 
    : Policies(proto.policies().begin(), proto.policies().end())
{}

TGetDatabaseStatusResult::TGetDatabaseStatusResult(TStatus&& status, const Ydb::Cms::GetDatabaseStatusResult& proto)
    : TStatus(std::move(status))
    , Path_(proto.path())
    , State_(ConvertState(proto.state()))
    , AllocatedResources_(proto.allocated_resources())
    , RegisteredResources_(proto.registered_resources().begin(), proto.registered_resources().end())
    , Generation_(proto.generation())
    , SchemaOperationQuotas_(proto.schema_operation_quotas())
    , DatabaseQuotas_(proto.database_quotas())
    , ScaleRecommenderPolicies_(proto.scale_recommender_policies())
{
    switch (proto.resources_kind_case()) {
        case Ydb::Cms::GetDatabaseStatusResult::kRequiredResources:
            ResourcesKind_ = TResources(proto.required_resources());
            break;
        case Ydb::Cms::GetDatabaseStatusResult::kRequiredSharedResources:
            ResourcesKind_ = TSharedResources(proto.required_shared_resources());
            break;
        case Ydb::Cms::GetDatabaseStatusResult::kServerlessResources:
            ResourcesKind_ = proto.serverless_resources();
            break;
        case Ydb::Cms::GetDatabaseStatusResult::RESOURCES_KIND_NOT_SET:
            ResourcesKind_ = std::monostate();
            break;
    }
}

const std::string& TGetDatabaseStatusResult::GetPath() const {
    return Path_;
}

EState TGetDatabaseStatusResult::GetState() const {
    return State_;
}

const TResourcesKind& TGetDatabaseStatusResult::GetResourcesKind() const {
    return ResourcesKind_;
}

const TResources& TGetDatabaseStatusResult::GetAllocatedResources() const {
    return AllocatedResources_;
}

const std::vector<TAllocatedComputationalUnit>& TGetDatabaseStatusResult::GetRegisteredResources() const {
    return RegisteredResources_;
}

std::uint64_t TGetDatabaseStatusResult::GetGeneration() const {
    return Generation_;
}

const TSchemaOperationQuotas& TGetDatabaseStatusResult::GetSchemaOperationQuotas() const {
    return SchemaOperationQuotas_;
}

const TDatabaseQuotas& TGetDatabaseStatusResult::GetDatabaseQuotas() const {
    return DatabaseQuotas_;
}

const TScaleRecommenderPolicies& TGetDatabaseStatusResult::GetScaleRecommenderPolicies() const {
    return ScaleRecommenderPolicies_;
}

void TGetDatabaseStatusResult::SerializeTo(Ydb::Cms::CreateDatabaseRequest& request) const {
    request.set_path(Path_);
    SerializeToImpl(ResourcesKind_, SchemaOperationQuotas_, DatabaseQuotas_, ScaleRecommenderPolicies_, request);
}

TCreateDatabaseSettings::TCreateDatabaseSettings(const Ydb::Cms::CreateDatabaseRequest& request)
  : SchemaOperationQuotas_(request.schema_operation_quotas())
  , DatabaseQuotas_(request.database_quotas())
  , ScaleRecommenderPolicies_(request.scale_recommender_policies())
{
  switch (request.resources_kind_case()) {
      case Ydb::Cms::CreateDatabaseRequest::kResources:
          ResourcesKind_ = TResources(request.resources());
          break;
      case Ydb::Cms::CreateDatabaseRequest::kSharedResources:
          ResourcesKind_ = TSharedResources(request.shared_resources());
          break;
      case Ydb::Cms::CreateDatabaseRequest::kServerlessResources:
          ResourcesKind_ = request.serverless_resources();
          break;
      case Ydb::Cms::CreateDatabaseRequest::RESOURCES_KIND_NOT_SET:
          ResourcesKind_ = std::monostate();
          break;
  }
}

void TCreateDatabaseSettings::SerializeTo(Ydb::Cms::CreateDatabaseRequest& request) const {
    SerializeToImpl(ResourcesKind_, SchemaOperationQuotas_, DatabaseQuotas_, ScaleRecommenderPolicies_, request);
}

class TCmsClient::TImpl : public TClientImplCommon<TCmsClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    { }

    TAsyncListDatabasesResult ListDatabases(const TListDatabasesSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Cms::ListDatabasesRequest>(settings);

        auto promise = NThreading::NewPromise<TListDatabasesResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Cms::ListDatabasesResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TListDatabasesResult val{TStatus(std::move(status)), result};
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Cms::V1::CmsService, Ydb::Cms::ListDatabasesRequest, Ydb::Cms::ListDatabasesResponse>(
            std::move(request),
            extractor,
            &Ydb::Cms::V1::CmsService::Stub::AsyncListDatabases,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncGetDatabaseStatusResult GetDatabaseStatus(const std::string& path, const TGetDatabaseStatusSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Cms::GetDatabaseStatusRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TGetDatabaseStatusResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Cms::GetDatabaseStatusResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TGetDatabaseStatusResult val{TStatus(std::move(status)), result};
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Cms::V1::CmsService, Ydb::Cms::GetDatabaseStatusRequest, Ydb::Cms::GetDatabaseStatusResponse>(
            std::move(request),
            extractor,
            &Ydb::Cms::V1::CmsService::Stub::AsyncGetDatabaseStatus,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus CreateDatabase(const std::string& path, const TCreateDatabaseSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Cms::CreateDatabaseRequest>(settings);
        request.set_path(path);
        settings.SerializeTo(request);

        return RunSimple<Ydb::Cms::V1::CmsService, Ydb::Cms::CreateDatabaseRequest, Ydb::Cms::CreateDatabaseResponse>(
            std::move(request),
            &Ydb::Cms::V1::CmsService::Stub::AsyncCreateDatabase,
            TRpcRequestSettings::Make(settings));
    }
};

TCmsClient::TCmsClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncListDatabasesResult TCmsClient::ListDatabases(const TListDatabasesSettings& settings) {
    return Impl_->ListDatabases(settings);
}

TAsyncGetDatabaseStatusResult TCmsClient::GetDatabaseStatus(
    const std::string& path,
    const TGetDatabaseStatusSettings& settings)
{
    return Impl_->GetDatabaseStatus(path, settings);
}

TAsyncStatus TCmsClient::CreateDatabase(
    const std::string& path,
    const TCreateDatabaseSettings& settings) 
{
    return Impl_->CreateDatabase(path, settings);
}

} // namespace NYdb::NCms
