#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrKqp {
class TQueryPhysicalGraph;
}

namespace NKikimr::NKqp {

// Description of a single PQ topic source that needs to be resolved.
struct TPqTopicResolverSource {
    TString Cluster;
    TString Endpoint;
    TString Database;     // real YDB database path for the describe RPC
    TString TopicPath;
    TString TokenName;    // key in SecureParams to look up the auth token
    bool    UseSsl = false;
    TString DatabaseForClusterConfig; // raw "database" field from the proto (may be cluster alias)
};

// Creates a TKqpPqTopicResolver actor.
//
// The actor describes every topic in `sources` in parallel using `pqGatewayFactory`,
// patches the partition counts in `*queryPhysicalGraph`, and then sends
// TEvKqpExecuter::TEvPqTopicResolveStatus back to `owner`.
//
// Must be called only after SecureParams is already populated.
//
// Parameters:
//   owner               – actor to notify when done
//   txId                – for logging
//   sources             – PQ topic sources collected during DoExecute()
//   secureParams        – populated SecureParams (token name → token value)
//   pqGatewayFactory    – used to create the gateway that does DescribeFederatedTopic
//   queryPhysicalGraph  – will be patched in-place with new partition counts
NActors::IActor* CreateKqpPqTopicResolver(
    const NActors::TActorId& owner,
    ui64 txId,
    TVector<TPqTopicResolverSource> sources,
    THashMap<TString, TString> secureParams,
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> queryPhysicalGraph);

} // namespace NKikimr::NKqp
