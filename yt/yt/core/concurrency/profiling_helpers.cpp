#include "profiling_helpers.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TTagSet GetThreadTags(
    const std::string& threadName)
{
    TTagSet tags;
    tags.AddTag(TTag("thread", threadName));
    return tags;
}

TTagSet GetBucketTags(
    const std::string& threadName,
    const std::string& bucketName)
{
    TTagSet tags;

    tags.AddTag(TTag("thread", threadName));
    tags.AddTag(TTag("bucket", bucketName), -1);

    return tags;
}

TTagSet GetQueueTags(
    const std::string& threadName,
    const std::string& bucketName,
    const std::string& queueName)
{
    TTagSet tags;

    tags.AddTag(TTag("thread", threadName));
    tags.AddTag(TTag("bucket", bucketName), -1);
    tags.AddTag(TTag("queue", queueName), -1);

    return tags;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
