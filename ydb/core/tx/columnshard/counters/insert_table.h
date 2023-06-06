#pragma once
#include "common_data.h"
#include "common/client.h"
#include "common/agent.h"

namespace NKikimr::NColumnShard {

class TPathIdClientCounters {
private:
    using TBase = TCommonCountersOwner;
    const std::shared_ptr<TValueAggregationClient> PathIdCommittedBytes;
    const std::shared_ptr<TValueAggregationClient> PathIdCommittedChunks;
public:
    TPathIdClientCounters(std::shared_ptr<TValueAggregationClient> pathIdCommittedBytes, std::shared_ptr<TValueAggregationClient> pathIdCommittedChunks)
        : PathIdCommittedBytes(pathIdCommittedBytes)
        , PathIdCommittedChunks(pathIdCommittedChunks) {
    }

    void OnPathIdDataInfo(const ui64 bytes, const ui32 chunks) const {
        PathIdCommittedBytes->Set(bytes);
        PathIdCommittedChunks->Set(chunks);
    }
};

class TPathIdInfoCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    const std::shared_ptr<TValueAggregationAgent> PathIdCommittedBytes;
    const std::shared_ptr<TValueAggregationAgent> PathIdCommittedChunks;
public:
    TPathIdInfoCounters(const TString& countersName, const TString& dataName)
        : TBase(countersName)
        , PathIdCommittedBytes(TBase::GetValueAutoAggregations("PathId/" + dataName + "/Bytes"))
        , PathIdCommittedChunks(TBase::GetValueAutoAggregations("PathId/" + dataName + "/Chunks"))
    {
    }

    TPathIdClientCounters GetClient() const {
        return TPathIdClientCounters(PathIdCommittedBytes->GetClient(PathIdCommittedBytes), PathIdCommittedChunks->GetClient(PathIdCommittedChunks));
    }
};

class TPathIdOwnedCounters {
public:
    const TPathIdClientCounters Inserted;
    const TPathIdClientCounters Committed;
    TPathIdOwnedCounters(TPathIdClientCounters&& inserted, TPathIdClientCounters&& committed)
        : Inserted(std::move(inserted))
        , Committed(std::move(committed)) {

    }
};

class TInsertTableCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    const TPathIdInfoCounters PathIdInserted;
    const TPathIdInfoCounters PathIdCommitted;
public:
    const TDataOwnerSignals Inserted;
    const TDataOwnerSignals Committed;
    const TDataOwnerSignals Aborted;

    TInsertTableCounters();

    TPathIdOwnedCounters GetPathIdCounters() const {
        return TPathIdOwnedCounters(PathIdInserted.GetClient(), PathIdCommitted.GetClient());
    }
};

}
