#ifndef FAIR_SHARE_ACTION_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_action_queue.h"
// For the sake of sane code completion.
#include "fair_share_action_queue.h"
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue>
class TEnumIndexedFairShareActionQueue
    : public IEnumIndexedFairShareActionQueue<EQueue>
{
public:
    TEnumIndexedFairShareActionQueue(
        const TString& threadName,
        const std::vector<TString>& queueNames,
        const THashMap<TString, std::vector<TString>>& queueToBucket)
        : Queue_(CreateFairShareActionQueue(threadName, queueNames, queueToBucket))
    { }

    const IInvokerPtr& GetInvoker(EQueue queue) override
    {
        return Queue_->GetInvoker(static_cast<int>(queue));
    }

    void Reconfigure(const THashMap<TString, double>& newBucketWeights) override
    {
        Queue_->Reconfigure(newBucketWeights);
    }

private:
    const IFairShareActionQueuePtr Queue_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename EQueue, typename EBucket>
IEnumIndexedFairShareActionQueuePtr<EQueue> CreateEnumIndexedFairShareActionQueue(
    const TString& threadName,
    const THashMap<EBucket, std::vector<EQueue>>& queueToBucket)
{
    std::vector<TString> queueNames;
    for (const auto& queueName : TEnumTraits<EQueue>::GetDomainNames()) {
        queueNames.push_back(TString{queueName});
    }
    THashMap<TString, std::vector<TString>> stringBuckets;
    for (const auto& [bucketName, bucket] : queueToBucket) {
        auto& stringBucket = stringBuckets[ToString(bucketName)];
        stringBucket.reserve(bucket.size());
        for (const auto& queue : bucket) {
            stringBucket.push_back(ToString(queue));
        }
    }
    return New<TEnumIndexedFairShareActionQueue<EQueue>>(threadName, queueNames, stringBuckets);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
