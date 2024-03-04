#pragma once
#include "read.h"
#include "write.h"
#include "remove_declare.h"
#include "remove_gc.h"
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NBlobOperations {

class TStorageCounters;

class TConsumerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    YDB_READONLY_DEF(std::shared_ptr<TReadCounters>, ReadCounters);
    YDB_READONLY_DEF(std::shared_ptr<TWriteCounters>, WriteCounters);
    YDB_READONLY_DEF(std::shared_ptr<TRemoveDeclareCounters>, RemoveDeclareCounters);
    YDB_READONLY_DEF(std::shared_ptr<TRemoveGCCounters>, RemoveGCCounters);
public:
    TConsumerCounters(const TString& consumerId, const TStorageCounters& parent);
};

class TStorageCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<TString, std::shared_ptr<TConsumerCounters>> ConsumerCounters;
public:
    TStorageCounters(const TString& storageId);

    std::shared_ptr<TConsumerCounters> GetConsumerCounter(const TString& consumerId);

};

}
