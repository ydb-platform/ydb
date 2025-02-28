#pragma once

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(const std::string& threadName);

NProfiling::TTagSet GetBucketTags(
    const std::string& threadName,
    const std::string& bucketName);

NProfiling::TTagSet GetQueueTags(
    const std::string& threadName,
    const std::string& bucketName,
    const std::string& queueName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
