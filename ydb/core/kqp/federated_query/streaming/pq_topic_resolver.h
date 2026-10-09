#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimrKqp {
class TQueryPhysicalGraph;
}

namespace NKikimr::NKqp {

// Creates a TKqpPqTopicResolver actor.
//
// The actor collects PQ source descriptors from the physical query of
// `queryPhysicalGraph`, describes every discovered topic in parallel using
// `pqGatewayFactory`, patches the partition counts in `*queryPhysicalGraph`,
// and then sends TEvStreamingQueryPrepared back to `owner`.
//
// Must be called only after SecureParams have been resolved (i.e. after secrets
// snapshot is obtained) so that the secureParams map is fully populated.
//
// Parameters:
//   owner               – actor to notify when done
//   txId                – for logging
//   database            – YDB database path used as a fallback when the topic
//                         source proto does not carry an explicit endpoint
//   secureParams        – populated SecureParams (source name → token value)
//   pqGatewayFactory    – used to create the gateway that does DescribeFederatedTopic
//   queryPhysicalGraph  – will be patched in-place with new partition counts
NActors::IActor* CreateKqpPqTopicResolver(
    const NActors::TActorId& owner,
    ui64 txId,
    const TString& database,
    THashMap<TString, TString> secureParams,
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> queryPhysicalGraph);

} // namespace NKikimr::NKqp
