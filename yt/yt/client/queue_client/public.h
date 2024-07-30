#pragma once

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ConsumerOffsetConflict)            (3100))
    ((InvalidEpoch)                      (3101))
    ((ZombieEpoch)                       (3102))
    ((InvalidRowSequenceNumbers)         (3103))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQueueRowset)

DECLARE_REFCOUNTED_STRUCT(IPersistentQueueRowset)

DECLARE_REFCOUNTED_STRUCT(IConsumerClient)
DECLARE_REFCOUNTED_STRUCT(ISubConsumerClient)

DECLARE_REFCOUNTED_STRUCT(IPartitionReader)
DECLARE_REFCOUNTED_CLASS(TPartitionReaderConfig)
DECLARE_REFCOUNTED_CLASS(TQueueStaticExportDestinationConfig)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TQueueProducerSessionId, TString);
YT_DEFINE_STRONG_TYPEDEF(TQueueProducerEpoch, i64);
YT_DEFINE_STRONG_TYPEDEF(TQueueProducerSequenceNumber, i64);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
