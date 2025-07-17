#pragma once
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/agent.h>
#include <ydb/library/signals/client.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {
class TPortionInfo;
class TPortionDataAccessor;

class TSimplePortionsGroupInfo {
private:
    using TCountByChannel = THashMap<ui16, i64>;
    TPositiveControlInteger BlobBytes;
    TPositiveControlInteger RawBytes;
    TPositiveControlInteger Count;
    TPositiveControlInteger RecordsCount;

protected:
    void Add(const TSimplePortionsGroupInfo& item) {
        BlobBytes.Add(item.BlobBytes);
        RawBytes.Add(item.RawBytes);
        Count.Add(item.Count);
        RecordsCount.Add(item.RecordsCount);
    }

public:
    ui64 GetCount() const {
        return Count.Val();
    }

    ui64 GetRecordsCount() const {
        return RecordsCount.Val();
    }

    ui64 GetBlobBytes() const {
        return BlobBytes.Val();
    }

    ui64 GetRawBytes() const {
        return RawBytes.Val();
    }

    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("blob_bytes", BlobBytes.Val());
        result.InsertValue("raw_bytes", RawBytes.Val());
        result.InsertValue("count", Count.Val());
        result.InsertValue("records_count", RecordsCount.Val());
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
        return TStringBuilder() << "{blob_bytes=" << BlobBytes.Val() << ";raw_bytes=" << RawBytes.Val() << ";count=" << Count.Val()
                                << ";records=" << RecordsCount.Val() << "}";
    }

    TSimplePortionsGroupInfo& operator+=(const TSimplePortionsGroupInfo& item) {
        Add(item);
        return *this;
    }

    TSimplePortionsGroupInfo operator+(const TSimplePortionsGroupInfo& item) const {
        TSimplePortionsGroupInfo result = *this;
        result += item;
        return result;
    }

    void AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
        AFL_VERIFY(p);
        AddPortion(*p);
    }

    void AddPortion(const TPortionInfo& p);

    void RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
        AFL_VERIFY(p);
        RemovePortion(*p);
    }

    void RemovePortion(const TPortionInfo& p);

    bool IsEmpty() const {
        if (!Count.Val()) {
            AFL_VERIFY(!BlobBytes.Val())("this", DebugString());
            AFL_VERIFY(!RawBytes.Val())("this", DebugString());
            AFL_VERIFY(!RecordsCount.Val())("this", DebugString());
            return true;
        }
        return false;
    }
};

class TFullPortionsGroupInfo: public TSimplePortionsGroupInfo {
private:
    using TBase = TSimplePortionsGroupInfo;
    using TCountByChannel = THashMap<ui16, i64>;
    TPositiveControlInteger Blobs;
    YDB_READONLY_DEF(TCountByChannel, BytesByChannel);

    void Add(const TFullPortionsGroupInfo& item) {
        TBase::Add(item);
        Blobs.Add(item.Blobs);
        for (const auto& [channel, bytes] : item.BytesByChannel) {
            BytesByChannel[channel] += bytes;
        }
    }

public:
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = TBase::SerializeToJson();
        result.InsertValue("blobs", Blobs.Val());
        {
            NJson::TJsonValue bytesByChannel = NJson::JSON_MAP;
            for (const auto& [channel, bytes] : BytesByChannel) {
                bytesByChannel.InsertValue(ToString(channel), bytes);
            }
            result.InsertValue("bytes_by_channel", std::move(bytesByChannel));
        }
        return result;
    }

    TString DebugString() const {
        return TBase::DebugString();
    }

    TFullPortionsGroupInfo& operator+=(const TFullPortionsGroupInfo& item) {
        Add(item);
        return *this;
    }

    TFullPortionsGroupInfo operator+(const TFullPortionsGroupInfo& item) const {
        TFullPortionsGroupInfo result = *this;
        result += item;
        return result;
    }

    void AddPortion(const TPortionDataAccessor& p);
    void RemovePortion(const TPortionDataAccessor& p);

    bool IsEmpty() const {
        if (TBase::IsEmpty()) {
            AFL_VERIFY(!Blobs)("this", DebugString());
            AFL_VERIFY(BytesByChannel.empty())("this", DebugString());
            return true;
        }
        return false;
    }
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

    void AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p);
    void RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p);
};

}   // namespace NKikimr::NColumnShard
