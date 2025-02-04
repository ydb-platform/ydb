#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb::Cms {
    class ListDatabasesResult;
    class GetDatabaseStatusResult;

    class StorageUnits;
    class ComputationalUnits;
    class AllocatedComputationalUnit;
    class ServerlessResources;
    class Resources;
    class SchemaOperationQuotas;
    class SchemaOperationQuotas_LeakyBucket;
    class DatabaseQuotas;
    class DatabaseQuotas_StorageQuotas;
    class ScaleRecommenderPolicies;
    class ScaleRecommenderPolicies_ScaleRecommenderPolicy;
    class ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy;
} // namespace Ydb::Cms

namespace NYdb::inline V3::NCms {

struct TListDatabasesSettings : public TSimpleRequestSettings<TListDatabasesSettings> {};

class TListDatabasesResult : public TStatus {
public:
    TListDatabasesResult(TStatus&& status, const Ydb::Cms::ListDatabasesResult& proto);
    const std::vector<std::string>& GetPaths() const;
private:
    std::vector<std::string> Paths_;
};

using TAsyncListDatabasesResult = NThreading::TFuture<TListDatabasesResult>;

struct TGetDatabaseStatusSettings : public TSimpleRequestSettings<TGetDatabaseStatusSettings> {
    FLUENT_SETTING(std::string, Path);
};

enum class EState {
    StateUnspecified = 0,
    Creating = 1,
    Running = 2,
    Removing = 3,
    PendingResources = 4,
    Configuring = 5,
};

struct TStorageUnits {
    TStorageUnits() = default;
    TStorageUnits(const Ydb::Cms::StorageUnits& proto);

    std::string UnitKind;
    ui64 Count;
};

struct TComputationalUnits {
    TComputationalUnits() = default;
    TComputationalUnits(const Ydb::Cms::ComputationalUnits& proto);

    std::string UnitKind;
    std::string AvailabilityZone;
    ui64 Count;
};

struct TAllocatedComputationalUnit {
    TAllocatedComputationalUnit() = default;
    TAllocatedComputationalUnit(const Ydb::Cms::AllocatedComputationalUnit& proto);

    std::string Host;
    ui32 Port;
    std::string UnitKind;
};

struct TResources {
    TResources() = default;
    TResources(const Ydb::Cms::Resources& proto);

    std::vector<TStorageUnits> StorageUnits;
    std::vector<TComputationalUnits> ComputationalUnits;
};

struct TSharedResources : public TResources {
    using TResources::TResources;
};

struct TServerlessResources {
    TServerlessResources() = default;
    TServerlessResources(const Ydb::Cms::ServerlessResources& proto);

    std::string SharedDatabasePath;
};

struct TSchemaOperationQuotas {
    struct TLeakyBucket {
        TLeakyBucket() = default;
        TLeakyBucket(const Ydb::Cms::SchemaOperationQuotas_LeakyBucket& proto);

        double BucketSize = 1;
        ui64 BucketSeconds = 2;
    };

    TSchemaOperationQuotas() = default;
    TSchemaOperationQuotas(const Ydb::Cms::SchemaOperationQuotas& proto);

    std::vector<TLeakyBucket> LeakyBucketQuotas;
};

struct TDatabaseQuotas {
    struct TStorageQuotas {
        TStorageQuotas() = default;
        TStorageQuotas(const Ydb::Cms::DatabaseQuotas_StorageQuotas& proto);

        std::string UnitKind;
        ui64 DataSizeHardQuota;
        ui64 DataSizeSoftQuota;
    };

    TDatabaseQuotas() = default;
    TDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& proto);

    ui64 DataSizeHardQuota;
    ui64 DataSizeSoftQuota;
    ui64 DataStreamShardsQuota;
    ui64 DataStreamReservedStorageQuota;
    ui32 TtlMinRunInternalSeconds;
    std::vector<TStorageQuotas> StorageQuotas;
};

struct TTargetTrackingPolicy {
    using TAverageCpuUtilizationPercent = ui32;

    TTargetTrackingPolicy() = default;
    TTargetTrackingPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy& proto);

    std::variant<TAverageCpuUtilizationPercent> Target;
};

struct TScaleRecommenderPolicy {
    TScaleRecommenderPolicy() = default;
    TScaleRecommenderPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy& proto);

    std::variant<TTargetTrackingPolicy> Policy;
};

struct TScaleRecommenderPolicies {
    TScaleRecommenderPolicies() = default;
    TScaleRecommenderPolicies(const Ydb::Cms::ScaleRecommenderPolicies& proto);

    std::vector<TScaleRecommenderPolicy> Policies;
};

class TGetDatabaseStatusResult : public TStatus {
public:
    TGetDatabaseStatusResult(TStatus&& status, const Ydb::Cms::GetDatabaseStatusResult& proto);

    const std::string& GetPath() const;
    EState GetState() const;
    const std::variant<TResources, TSharedResources, TServerlessResources>& GetResourcesKind() const;
    const TResources& GetAllocatedResources() const;
    const std::vector<TAllocatedComputationalUnit>& GetRegisteredResources() const;
    ui64 GetGeneration() const;
    const TSchemaOperationQuotas& GetSchemaOperationQuotas() const;
    const TDatabaseQuotas& GetDatabaseQuotas() const;
    const TScaleRecommenderPolicies& GetScaleRecommenderPolicies() const;

private:
    std::string Path_;
    EState State_;
    std::variant<TResources, TSharedResources, TServerlessResources> ResourcesKind_;
    TResources AllocatedResources_;
    std::vector<TAllocatedComputationalUnit> RegisteredResources_;
    ui64 Generation_;
    TSchemaOperationQuotas SchemaOperationQuotas_;
    TDatabaseQuotas DatabaseQuotas_;
    TScaleRecommenderPolicies ScaleRecommenderPolicies;
};

using TAsyncGetDatabaseStatusResult = NThreading::TFuture<TGetDatabaseStatusResult>;

class TCmsClient {
public:
    explicit TCmsClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncListDatabasesResult ListDatabases(const TListDatabasesSettings& settings = TListDatabasesSettings());
    TAsyncGetDatabaseStatusResult GetDatabaseStatus(const TGetDatabaseStatusSettings& settings = TGetDatabaseStatusSettings());
    
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline V3::NCms
