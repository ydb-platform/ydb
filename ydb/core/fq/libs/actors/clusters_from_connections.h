#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>

namespace NFq {

NYql::TPqClusterConfig CreatePqClusterConfig(const TString& name, bool useBearerForYdb, const TString& authToken, const TString& accountSignature, const FederatedQuery::DataStreams& ds);

NYql::TS3ClusterConfig CreateS3ClusterConfig(const TString& name, const TString& authToken, const TString& objectStorageEndpoint, const TString& accountSignature, const FederatedQuery::ObjectStorageConnection& s3);

NYql::TSolomonClusterConfig CreateSolomonClusterConfig(const TString& name, const TString& authToken, const TString& endpoint, const TString& accountSignature, const FederatedQuery::Monitoring& monitoring);

void AddClustersFromConnections(const NConfig::TCommonConfig& common,
    const THashMap<TString, FederatedQuery::Connection>& connections,
    const TString& monitoringEndpoint,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures,
    NYql::TGatewaysConfig& gatewaysConfig,
    THashMap<TString, TString>& clusters);

} //NFq
