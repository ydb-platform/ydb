#include "storage.h"
#include <util/generic/serialized_enum.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations {

TStorageCounters::TStorageCounters(const TString& storageId)
    : TBase("BlobStorages")
{
    DeepSubGroup("StorageId", storageId);
    Consumers.resize((ui32)EConsumer::COUNT);
    for (auto&& i : GetEnumAllValues<EConsumer>()) {
        if (i == EConsumer::COUNT) {
            continue;
        }
        Consumers[(ui32)i] = std::make_shared<TConsumerCounters>(::ToString(i), *this);
    }
}

std::shared_ptr<NKikimr::NOlap::NBlobOperations::TConsumerCounters> TStorageCounters::GetConsumerCounter(const EConsumer consumer) {
    AFL_VERIFY((ui32)consumer < Consumers.size());
    return Consumers[(ui32)consumer];
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
