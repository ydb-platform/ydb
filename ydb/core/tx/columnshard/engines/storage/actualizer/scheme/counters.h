#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NActualizer {

class TSchemeGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeInternalWrite;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeExternalWrite;
public:
    TSchemeGlobalCounters()
        : TBase("SchemeActualizer")
    {
        QueueSizeExternalWrite = TBase::GetValueAutoAggregations("Granule/Scheme/Actualization/QueueSize/ExternalWrite");
        QueueSizeInternalWrite = TBase::GetValueAutoAggregations("Granule/Scheme/Actualization/QueueSize/InternalWrite");
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeExternalWrite() {
        return Singleton<TSchemeGlobalCounters>()->QueueSizeExternalWrite->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeInternalWrite() {
        return Singleton<TSchemeGlobalCounters>()->QueueSizeInternalWrite->GetClient();
    }

};

class TSchemeCounters {
public:
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeInternalWrite;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeExternalWrite;

    TSchemeCounters()
        : QueueSizeInternalWrite(TSchemeGlobalCounters::BuildQueueSizeInternalWrite())
        , QueueSizeExternalWrite(TSchemeGlobalCounters::BuildQueueSizeExternalWrite())
{
    }

};

}
