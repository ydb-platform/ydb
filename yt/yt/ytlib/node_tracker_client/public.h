#pragma once

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRegisterNode;
class TRspRegisterNode;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectoryBuilder;

DECLARE_REFCOUNTED_STRUCT(INodeDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TNodeDirectorySynchronizerConfig)

DECLARE_REFCOUNTED_STRUCT(INodeChannelFactory)

DECLARE_REFCOUNTED_STRUCT(INodeStatusDirectory)

DEFINE_ENUM(EMemoryLimitType,
    ((None)                        (0))
    ((Static)                      (1))
    ((Dynamic)                     (2))
);

DEFINE_ENUM(ENodeState,
    // Used internally.
    ((Unknown)      (-1))
    // Not registered.
    ((Offline)       (0))
    // Registered but did not report some of the heartbeats.
    ((Registered)    (1))
    // Registered and reported all the expected types of heartbeats
    // at least once.
    ((Online)        (2))
    // Unregistered and placed into disposal queue.
    ((Unregistered)  (3))
    // Indicates that state varies across cells.
    ((Mixed)         (4))
    // Unregistered and ongoing disposal.
    ((BeingDisposed) (5))
);

DEFINE_ENUM(ENodeRole,
    ((MasterCache)       (0))
    ((TimestampProvider) (1))
);

DEFINE_ENUM(ENodeFlavor,
    // COMPAT(gritukan)
    ((Cluster)      (0))
    // Node that is used to store chunks.
    ((Data)         (1))
    // Node that is used to execute jobs.
    ((Exec)         (2))
    // Node that is used to host dynamic tables tablets.
    ((Tablet)       (3))
    // Node that is used to host chaos cells.
    ((Chaos)        (4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
