#pragma once 

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>

namespace NFq {

class TIcebergProcessor {
public:
    explicit TIcebergProcessor(const FederatedQuery::Iceberg& config);

    TIcebergProcessor(const FederatedQuery::Iceberg& config, NYql::TIssues& issues);

    ~TIcebergProcessor() = default;

    void Process();

    void ProcessSkipAuth();

    void SetDoOnWarehouseS3(std::function<
        void(const FederatedQuery::IcebergWarehouse_S3&, const TString&)> callback) {
        OnS3Callback_ = callback;
    }

    void SetDoOnCatalogHive(std::function<
        void(const FederatedQuery::IcebergCatalog_HiveMetastore&)> callback) {
        OnHiveCallback_ = callback;
    }

    void SetDoOnCatalogHadoop(std::function<
        void(const FederatedQuery::IcebergCatalog_Hadoop&)> callback) {
        OnHadoopCallback_ = callback;
    }

private:
    void DoOnPropertyRequiredError(const TString& property);

    void DoOnError(const TString& property, const TString& msg);

    void ProcessWarehouse(const FederatedQuery::IcebergWarehouse& warehouse);

    void ProcessWarehouseS3(const FederatedQuery::IcebergWarehouse_S3& s3);

    void ProcessCatalog(const FederatedQuery::IcebergCatalog& catalog);

    void ProcessCatalogHadoop(const FederatedQuery::IcebergCatalog_Hadoop& hadoop);

    void ProcessCatalogHiveMetastore(const FederatedQuery::IcebergCatalog_HiveMetastore& hive);

    bool HasErrors() const {
        return Issues_ && !Issues_->Empty();
    }

private:
    const FederatedQuery::Iceberg& Config_;
    NYql::TIssues* Issues_;
    std::function<void(const FederatedQuery::IcebergWarehouse_S3&, const TString&)> OnS3Callback_;
    std::function<void(const FederatedQuery::IcebergCatalog_HiveMetastore&)> OnHiveCallback_;
    std::function<void(const FederatedQuery::IcebergCatalog_Hadoop&)> OnHadoopCallback_;
};

TString MakeIcebergCreateExternalDataSourceProperties(const NConfig::TCommonConfig& yqConfig,
                                                      const FederatedQuery::Iceberg& config);

void FillIcebergGenericClusterConfig(const NConfig::TCommonConfig& yqConfig,
                                     const FederatedQuery::Iceberg& config,
                                     NYql::TGenericClusterConfig& cluster);

} // NFq
