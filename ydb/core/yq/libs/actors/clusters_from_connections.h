#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/public/api/protos/yq.pb.h>

namespace NYq {

NYql::TPqClusterConfig CreatePqClusterConfig(const TString& name, bool useBearerForYdb, const TString& authToken, const TString& accountSignature, const YandexQuery::DataStreams& ds);

NYql::TS3ClusterConfig CreateS3ClusterConfig(const TString& name, const TString& authToken, const TString& objectStorageEndpoint, const TString& accountSignature, const YandexQuery::ObjectStorageConnection& s3);

NYql::TSolomonClusterConfig CreateSolomonClusterConfig(const TString& name, const TString& authToken, const TString& endpoint, const TString& accountSignature, const YandexQuery::Monitoring& monitoring);

void AddClustersFromConnections(const THashMap<TString, YandexQuery::Connection>& connections,
    bool useBearerForYdb,
    const TString& objectStorageEndpoint,
    const TString& monitoringEndpoint,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures,
    NYql::TGatewaysConfig& gatewaysConfig,
    THashMap<TString, TString>& clusters);

} //NYq
