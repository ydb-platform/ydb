#pragma once
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/counters/counters.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

static const ui64 SmallPortionDetectSizeLimit = 1 << 20;
static const ui64 MinBucketSize = 8 << 20;

TDuration GetCommonFreshnessCheckDuration();

class TSimplePortionsGroupInfo {
private:
    YDB_READONLY(i64, Bytes, 0);
    YDB_READONLY(i64, Count, 0);
    YDB_READONLY(i64, RecordsCount, 0);

public:
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("bytes", Bytes);
        result.InsertValue("count", Count);
        result.InsertValue("records_count", RecordsCount);
        return result;
    }

    TString DebugString() const {
        return TStringBuilder() << "{bytes=" << Bytes << ";count=" << Count << ";records=" << RecordsCount << "}";
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        Bytes += p->GetTotalBlobBytes();
        Count += 1;
        RecordsCount += p->NumRows();
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        Bytes -= p->GetTotalBlobBytes();
        Count -= 1;
        RecordsCount -= p->NumRows();
        AFL_VERIFY(Bytes >= 0);
        AFL_VERIFY(Count >= 0);
        AFL_VERIFY(RecordsCount >= 0);
    }
};

class TPortionsGroupInfo: public TSimplePortionsGroupInfo {
private:
    using TBase = TSimplePortionsGroupInfo;
    std::shared_ptr<TPortionCategoryCounters> Signals;

public:
    TPortionsGroupInfo(const std::shared_ptr<TPortionCategoryCounters>& signals)
        : Signals(signals) {
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        TBase::AddPortion(p);
        Signals->AddPortion(p);
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        TBase::RemovePortion(p);
        Signals->RemovePortion(p);
    }
};

class TBucketPortionInfo {
private:
    YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, PortionInfo);
    YDB_READONLY(ui64, TotalBlobBytes, 0);
public:
    const TPortionInfo* operator->() const {
        return PortionInfo.get();
    }

    TPortionInfo& MutablePortionInfo() const {
        return *PortionInfo;
    }

    TBucketPortionInfo(const std::shared_ptr<TPortionInfo>& portion)
        : PortionInfo(portion)
        , TotalBlobBytes(portion->GetTotalBlobBytes())
    {

    }
};

class TPKPortions {
public:
    using TBucketPortions = THashMap<ui64, TBucketPortionInfo>;
private:
    YDB_READONLY_DEF(TBucketPortions, Start);
    YDB_READONLY_DEF(TBucketPortions, Finish);
public:
    bool IsEmpty() const {
        return Start.empty() && Finish.empty();
    }
    void AddStart(const TBucketPortionInfo& portion) {
        AFL_VERIFY(Start.emplace(portion->GetPortionId(), portion).second);
    }
    void AddFinish(const TBucketPortionInfo& portion) {
        AFL_VERIFY(Finish.emplace(portion->GetPortionId(), portion).second);
    }
    void RemoveStart(const TBucketPortionInfo& portion) {
        AFL_VERIFY(Start.erase(portion->GetPortionId()));
    }
    void RemoveFinish(const TBucketPortionInfo& portion) {
        AFL_VERIFY(Finish.erase(portion->GetPortionId()));
    }
};

class TBucketInfo {
protected:
    std::map<NArrow::TReplaceKey, TPKPortions> PKPortions;
    std::map<TInstant, THashMap<ui64, TBucketPortionInfo>> SnapshotPortions;
public:
    const std::map<NArrow::TReplaceKey, TPKPortions>& GetPKPortions() const {
        return PKPortions;
    }
    const std::map<TInstant, THashMap<ui64, TBucketPortionInfo>>& GetSnapshotPortions() const {
        return SnapshotPortions;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
