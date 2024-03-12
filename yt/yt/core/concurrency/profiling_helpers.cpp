#include "profiling_helpers.h"

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TTagSet GetThreadTags(
    const TString& threadName)
{
    TTagSet tags;
    tags.AddTag(std::pair<TString, TString>("thread", threadName));
    return tags;
}

TTagSet GetBucketTags(
    const TString& threadName,
    const TString& bucketName)
{
    TTagSet tags;

    tags.AddTag(std::pair<TString, TString>("thread", threadName));
    tags.AddTag(std::pair<TString, TString>("bucket", bucketName), -1);

    return tags;
}

TTagSet GetQueueTags(
    const TString& threadName,
    const TString& bucketName,
    const TString& queueName)
{
    TTagSet tags;

    tags.AddTag(std::pair<TString, TString>("thread", threadName));
    tags.AddTag(std::pair<TString, TString>("bucket", bucketName), -1);
    tags.AddTag(std::pair<TString, TString>("queue", queueName), -1);

    return tags;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

