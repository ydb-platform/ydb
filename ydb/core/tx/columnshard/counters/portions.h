#pragma once
#include "common/agent.h"
#include "common/client.h"
#include "common/owner.h"

#include <library/cpp/json/writer/json_value.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {
class TPortionInfo;

class TSimplePortionsGroupInfo {
private:
    YDB_READONLY(i64, BlobBytes, 0);
    YDB_READONLY(i64, RawBytes, 0);
    YDB_READONLY(i64, Count, 0);
    YDB_READONLY(i64, RecordsCount, 0);
    YDB_READONLY(i64, ChunksCount, 0);

public:
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("blob_bytes", BlobBytes);
        result.InsertValue("raw_bytes", RawBytes);
        result.InsertValue("count", Count);
        result.InsertValue("records_count", RecordsCount);
        result.InsertValue("chunks_count", ChunksCount);
        return result;
    }

    ui64 PredictPackedBlobBytes(const std::optional<double> kff) const {
        if (kff) {
            return RawBytes * *kff;
        } else {
            return BlobBytes;
        }
    }

    TString DebugString() const {
        return TStringBuilder() << "{blob_bytes=" << BlobBytes << ";raw_bytes=" << RawBytes << ";count=" << Count << ";records=" << RecordsCount
                                << "}";
    }

    TSimplePortionsGroupInfo operator+(const TSimplePortionsGroupInfo& item) const {
        TSimplePortionsGroupInfo result;
        result.BlobBytes = BlobBytes + item.BlobBytes;
        result.RawBytes = RawBytes + item.RawBytes;
        result.Count = Count + item.Count;
        result.RecordsCount = RecordsCount + item.RecordsCount;
        result.ChunksCount = ChunksCount + item.ChunksCount;
        return result;
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p);
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p);

    void AddPortion(const TPortionInfo& p);
    void RemovePortion(const TPortionInfo& p);
};

class TPortionGroupCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr Count;
    NMonitoring::TDynamicCounters::TCounterPtr RawBytes;
    NMonitoring::TDynamicCounters::TCounterPtr BlobBytes;

public:
    TPortionGroupCounters(const TString& kind, const NColumnShard::TCommonCountersOwner& baseOwner)
        : TBase(baseOwner, "kind", kind) {
        Count = TBase::GetDeriviative("Portions/Count");
        RawBytes = TBase::GetDeriviative("Portions/Raw/Bytes");
        BlobBytes = TBase::GetDeriviative("Portions/Blob/Bytes");
    }

    void OnData(const i64 portionsCount, const i64 portionBlobBytes, const i64 portionRawBytes) {
        Count->Add(portionsCount);
        RawBytes->Add(portionRawBytes);
        BlobBytes->Add(portionBlobBytes);
    }

    void OnData(const TSimplePortionsGroupInfo& group) {
        Count->Add(group.GetCount());
        RawBytes->Add(group.GetRawBytes());
        BlobBytes->Add(group.GetBlobBytes());
    }
};

}   // namespace NKikimr::NOlap

namespace NKikimr::NColumnShard {

class TPortionCategoryCounterAgents: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

public:
    const std::shared_ptr<TValueAggregationAgent> RecordsCount;
    const std::shared_ptr<TValueAggregationAgent> Count;
    const std::shared_ptr<TValueAggregationAgent> BlobBytes;
    const std::shared_ptr<TValueAggregationAgent> RawBytes;
    TPortionCategoryCounterAgents(TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, "category", categoryName)
        , RecordsCount(TBase::GetValueAutoAggregations("ByGranule/Portions/RecordsCount"))
        , Count(TBase::GetValueAutoAggregations("ByGranule/Portions/Count"))
        , BlobBytes(TBase::GetValueAutoAggregations("ByGranule/Portions/Blob/Bytes"))
        , RawBytes(TBase::GetValueAutoAggregations("ByGranule/Portions/Raw/Bytes")) {
    }
};

class TPortionCategoryCounters {
private:
    std::shared_ptr<TValueAggregationClient> RecordsCount;
    std::shared_ptr<TValueAggregationClient> Count;
    std::shared_ptr<TValueAggregationClient> BlobBytes;
    std::shared_ptr<TValueAggregationClient> RawBytes;

public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents) {
        RecordsCount = agents.RecordsCount->GetClient();
        Count = agents.Count->GetClient();
        BlobBytes = agents.BlobBytes->GetClient();
        RawBytes = agents.RawBytes->GetClient();
    }

    void AddPortion(const std::shared_ptr<NOlap::TPortionInfo>& p);

    void RemovePortion(const std::shared_ptr<NOlap::TPortionInfo>& p);
};

}   // namespace NKikimr::NColumnShard
