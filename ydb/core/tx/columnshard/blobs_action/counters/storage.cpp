#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

TStorageCounters::TStorageCounters(const TString& storageId)
    : TBase("BlobStorages")
{
    DeepSubGroup("StorageId", storageId);
}

std::shared_ptr<NKikimr::NOlap::NBlobOperations::TConsumerCounters> TStorageCounters::GetConsumerCounter(const TString& consumerId) {
    auto it = ConsumerCounters.find(consumerId);
    if (it == ConsumerCounters.end()) {
        it = ConsumerCounters.emplace(consumerId, std::make_shared<TConsumerCounters>(consumerId, *this)).first;
    }
    return it->second;
}

TConsumerCounters::TConsumerCounters(const TString& consumerId, const TStorageCounters& parent)
    : TBase(parent)
{
    DeepSubGroup("Consumer", consumerId);
    ReadCounters = std::make_shared<TReadCounters>(*this);
    WriteCounters = std::make_shared<TWriteCounters>(*this);
    RemoveDeclareCounters = std::make_shared<TRemoveDeclareCounters>(*this);
    RemoveGCCounters = std::make_shared<TRemoveGCCounters>(*this);
}

}
