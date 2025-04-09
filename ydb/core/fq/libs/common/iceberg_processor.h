#pragma once 

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NFq {

class TIcebergProcessor {
public:
    explicit TIcebergProcessor(const FederatedQuery::Iceberg& config);

    TIcebergProcessor(const FederatedQuery::Iceberg& config, NYql::TIssues& issues);

    ~TIcebergProcessor() = default;

    void Process();

    void ProcessSkipAuth();

    void SetDoOnWarehouseS3(std::function<
        void(const FederatedQuery::TIcebergWarehouse_TS3&)> callback) {
        OnS3Callback_ = callback;
    }

    void SetDoOnCatalogHive(std::function<
        void(const FederatedQuery::TIcebergCatalog_THive&)> callback) {
        OnHiveCallback_ = callback;
    }

    void SetDoOnCatalogHadoop(std::function<
        void(const FederatedQuery::TIcebergCatalog_THadoop&)> callback) {
        OnHadoopCallback_ = callback;
    }

    std::vector<NYql::TIssue> GetIssues() const;

private:
    void RiseError(const TString& field,const TString& msg);

    void ProcessWarehouse(const FederatedQuery::TIcebergWarehouse& warehouse);

    void ProcessWarehouseS3();

    void ProcessCatalog(const FederatedQuery::TIcebergCatalog& catalog);

    void ProcessCatalogHadoop(const FederatedQuery::TIcebergCatalog_THadoop& hadoop);

    void ProcessCatalogHive(const FederatedQuery::TIcebergCatalog_THive& hive);

    bool GetHasErrors() {
        return Issues_ ? Issues_->Size() > 0 : false;
    }

private:
    const FederatedQuery::Iceberg& Config_;
    NYql::TIssues* Issues_;
    std::function<void(const FederatedQuery::TIcebergWarehouse_TS3&)> OnS3Callback_;
    std::function<void(const FederatedQuery::TIcebergCatalog_THive&)> OnHiveCallback_;
    std::function<void(const FederatedQuery::TIcebergCatalog_THadoop&)> OnHadoopCallback_;
};

TString MakeIcebergCreateExternalDataSourceProperties(const FederatedQuery::Iceberg& config, bool useTls);

void FillIcebergGenericClusterConfig(const FederatedQuery::Iceberg& config, ::NYql::TGenericClusterConfig& cluster);

} // NFq
