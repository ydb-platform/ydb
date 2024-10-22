#pragma once
#include "common_data.h"
#include "common/client.h"
#include "common/agent.h"

namespace NKikimr::NColumnShard {

class TPathIdClientCounters {
private:
    using TBase = TCommonCountersOwner;
    const std::shared_ptr<TValueAggregationClient> PathIdBytes;
    const std::shared_ptr<TValueAggregationClient> PathIdChunks;
public:
    TPathIdClientCounters(std::shared_ptr<TValueAggregationClient> pathIdCommittedBytes, std::shared_ptr<TValueAggregationClient> pathIdCommittedChunks)
        : PathIdBytes(pathIdCommittedBytes)
        , PathIdChunks(pathIdCommittedChunks) {
    }

    void OnPathIdDataInfo(const ui64 bytes, const ui32 chunks) const {
        PathIdBytes->SetValue(bytes);
        PathIdChunks->SetValue(chunks);
    }
};

class TPathIdInfoCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    const std::shared_ptr<TValueAggregationAgent> PathIdBytes;
    const std::shared_ptr<TValueAggregationAgent> PathIdChunks;
public:
    TPathIdInfoCounters(const TString& countersName, const TString& dataName)
        : TBase(countersName)
        , PathIdBytes(TBase::GetValueAutoAggregations("PathId/" + dataName + "/Bytes"))
        , PathIdChunks(TBase::GetValueAutoAggregations("PathId/" + dataName + "/Chunks"))
    {
    }

    TPathIdClientCounters GetClient() const {
        return TPathIdClientCounters(PathIdBytes->GetClient(), PathIdChunks->GetClient());
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
