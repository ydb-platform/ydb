#pragma once

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((ConsumerOffsetConflict)            (3100))
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

} // namespace NYT::NQueueClient
