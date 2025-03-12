#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb::Cms {
    class CreateDatabaseRequest;
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

namespace NYdb::inline Dev::NCms {

struct TListDatabasesSettings : public TOperationRequestSettings<TListDatabasesSettings> {};

class TListDatabasesResult : public TStatus {
public:
    TListDatabasesResult(TStatus&& status, const Ydb::Cms::ListDatabasesResult& proto);
    const std::vector<std::string>& GetPaths() const;
private:
    std::vector<std::string> Paths_;
};

using TAsyncListDatabasesResult = NThreading::TFuture<TListDatabasesResult>;

struct TGetDatabaseStatusSettings : public TOperationRequestSettings<TGetDatabaseStatusSettings> {};

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
    std::uint64_t Count;
};

struct TComputationalUnits {
    TComputationalUnits() = default;
    TComputationalUnits(const Ydb::Cms::ComputationalUnits& proto);

    std::string UnitKind;
    std::string AvailabilityZone;
    std::uint64_t Count;
};

struct TAllocatedComputationalUnit {
    TAllocatedComputationalUnit() = default;
    TAllocatedComputationalUnit(const Ydb::Cms::AllocatedComputationalUnit& proto);

    std::string Host;
    std::uint32_t Port;
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
        std::uint64_t BucketSeconds = 2;
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
        std::uint64_t DataSizeHardQuota;
        std::uint64_t DataSizeSoftQuota;
    };

    TDatabaseQuotas() = default;
    TDatabaseQuotas(const Ydb::Cms::DatabaseQuotas& proto);

    std::uint64_t DataSizeHardQuota;
    std::uint64_t DataSizeSoftQuota;
    std::uint64_t DataStreamShardsQuota;
    std::uint64_t DataStreamReservedStorageQuota;
    std::uint32_t TtlMinRunInternalSeconds;
    std::vector<TStorageQuotas> StorageQuotas;
};

struct TTargetTrackingPolicy {
    using TAverageCpuUtilizationPercent = std::uint32_t;

    TTargetTrackingPolicy() = default;
    TTargetTrackingPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy_TargetTrackingPolicy& proto);

    std::variant<std::monostate, TAverageCpuUtilizationPercent> Target;
};

struct TScaleRecommenderPolicy {
    TScaleRecommenderPolicy() = default;
    TScaleRecommenderPolicy(const Ydb::Cms::ScaleRecommenderPolicies_ScaleRecommenderPolicy& proto);

    std::variant<std::monostate, TTargetTrackingPolicy> Policy;
};

struct TScaleRecommenderPolicies {
    TScaleRecommenderPolicies() = default;
    TScaleRecommenderPolicies(const Ydb::Cms::ScaleRecommenderPolicies& proto);

    std::vector<TScaleRecommenderPolicy> Policies;
};

using TResourcesKind = std::variant<std::monostate, TResources, TSharedResources, TServerlessResources>;

class TGetDatabaseStatusResult : public TStatus {
public:
    TGetDatabaseStatusResult(TStatus&& status, const Ydb::Cms::GetDatabaseStatusResult& proto);

    const std::string& GetPath() const;
    EState GetState() const;
    const TResourcesKind& GetResourcesKind() const;
    const TResources& GetAllocatedResources() const;
    const std::vector<TAllocatedComputationalUnit>& GetRegisteredResources() const;
    std::uint64_t GetGeneration() const;
    const TSchemaOperationQuotas& GetSchemaOperationQuotas() const;
    const TDatabaseQuotas& GetDatabaseQuotas() const;
    const TScaleRecommenderPolicies& GetScaleRecommenderPolicies() const;

    // Fills CreateDatabaseRequest proto from this database status
    void SerializeTo(Ydb::Cms::CreateDatabaseRequest& request) const;

private:
    std::string Path_;
    EState State_;
    TResourcesKind ResourcesKind_;
    TResources AllocatedResources_;
    std::vector<TAllocatedComputationalUnit> RegisteredResources_;
    std::uint64_t Generation_;
    TSchemaOperationQuotas SchemaOperationQuotas_;
    TDatabaseQuotas DatabaseQuotas_;
    TScaleRecommenderPolicies ScaleRecommenderPolicies_;
};

using TAsyncGetDatabaseStatusResult = NThreading::TFuture<TGetDatabaseStatusResult>;

struct TCreateDatabaseSettings : public TOperationRequestSettings<TCreateDatabaseSettings> {
    TCreateDatabaseSettings() = default;
    explicit TCreateDatabaseSettings(const Ydb::Cms::CreateDatabaseRequest& request);

    // Fills CreateDatabaseRequest proto from this settings
    void SerializeTo(Ydb::Cms::CreateDatabaseRequest& request) const;

    FLUENT_SETTING(TResourcesKind, ResourcesKind);
    FLUENT_SETTING(TSchemaOperationQuotas, SchemaOperationQuotas);
    FLUENT_SETTING(TDatabaseQuotas, DatabaseQuotas);
    FLUENT_SETTING(TScaleRecommenderPolicies, ScaleRecommenderPolicies);
};

class TCmsClient {
public:
    explicit TCmsClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncListDatabasesResult ListDatabases(const TListDatabasesSettings& settings = TListDatabasesSettings());
    TAsyncGetDatabaseStatusResult GetDatabaseStatus(const std::string& path,
        const TGetDatabaseStatusSettings& settings = TGetDatabaseStatusSettings());
    TAsyncStatus CreateDatabase(const std::string& path,
        const TCreateDatabaseSettings& settings = TCreateDatabaseSettings());
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev::NCms
