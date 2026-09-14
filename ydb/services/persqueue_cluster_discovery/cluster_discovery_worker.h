#pragma once

#include "counters.h"
#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>
#include <ydb/core/util/address_classifier.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NPQ::NClusterDiscovery::NWorker {

using namespace NActors;
using namespace NAddressClassifier;
using namespace NGRpcService;
using namespace NPQ::NClusterTracker;
using namespace NCounters;

IActor* CreateClusterDiscoveryWorker(THolder<NGRpcService::TEvDiscoverPQClustersRequest> ev,
                                     TLabeledAddressClassifier::TConstPtr datacenterClassifier,
                                     TLabeledAddressClassifier::TConstPtr cloudNetsClassifier,
                                     TClustersList::TConstPtr clustersList,
                                     TClusterDiscoveryCounters::TPtr counters);

} // namespace NKikimr::NClusterDiscovery::NWorker
