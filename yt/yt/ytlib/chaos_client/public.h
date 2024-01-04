#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellDescriptorLite;
struct TAlienPeerDescriptor;
struct TAlienCellDescriptor;

DECLARE_REFCOUNTED_STRUCT(IChaosCellDirectorySynchronizer)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IChaosCellChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardResidencyCache)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTracker)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTrackerCache)
DECLARE_REFCOUNTED_CLASS(TChaosCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationCardResidencyCacheConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationCardChannelConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
