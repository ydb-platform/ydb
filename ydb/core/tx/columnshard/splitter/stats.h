#pragma once
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <optional>
#include <map>
#include <deque>
#include <string>
#include <memory>

namespace NKikimr::NOlap {

class TSimpleSerializationStat {
protected:
    ui64 SerializedBytes = 0;
    ui64 RecordsCount = 0;
    ui64 RawBytes = 0;
public:
    TSimpleSerializationStat() = default;
    TSimpleSerializationStat(const ui64 bytes, const ui64 recordsCount, const ui64 rawBytes)
        : SerializedBytes(bytes)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
    {
        Y_VERIFY(SerializedBytes);
        Y_VERIFY(RecordsCount);
        Y_VERIFY(RawBytes);
    }

    ui64 GetSerializedBytes() const{
        return SerializedBytes;
    }
    ui64 GetRecordsCount() const{
        return RecordsCount;
    }
    ui64 GetRawBytes() const {
        return RawBytes;
    }

    void AddStat(const TSimpleSerializationStat& stat) {
        SerializedBytes += stat.SerializedBytes;
        RecordsCount += stat.RecordsCount;
        RawBytes += stat.RawBytes;
    }

    void RemoveStat(const TSimpleSerializationStat& stat) {
        Y_VERIFY(SerializedBytes >= stat.SerializedBytes);
        SerializedBytes -= stat.SerializedBytes;
        Y_VERIFY(RecordsCount >= stat.RecordsCount);
        RecordsCount -= stat.RecordsCount;
        Y_VERIFY(RawBytes >= stat.RawBytes);
        RawBytes -= stat.RawBytes;
    }

    double GetPackedRecordSize() const {
        return (double)SerializedBytes / RecordsCount;
    }

    std::optional<ui64> PredictOptimalPackRecordsCount(const ui64 recordsCount, const ui64 blobSize) const {
        if (!RecordsCount) {
            return {};
        }
        const ui64 fullSize = 1.0 * recordsCount / RecordsCount * SerializedBytes;
        if (fullSize < blobSize) {
            return recordsCount;
        } else {
            return std::floor(1.0 * blobSize / SerializedBytes * RecordsCount);
        }
    }

    std::optional<ui64> PredictOptimalSplitFactor(const ui64 recordsCount, const ui64 blobSize) const {
        if (!RecordsCount) {
            return {};
        }
        const ui64 fullSize = 1.0 * recordsCount / RecordsCount * SerializedBytes;
        if (fullSize < blobSize) {
            return 1;
        } else {
            return std::floor(1.0 * fullSize / blobSize);
        }
    }
};

class TBatchSerializationStat: public TSimpleSerializationStat {
private:
    using TBase = TSimpleSerializationStat;
public:
    using TBase::TBase;
    TBatchSerializationStat(const TSimpleSerializationStat& item)
        : TBase(item)
    {

    }

    void Merge(const TSimpleSerializationStat& item) {
        SerializedBytes += item.GetSerializedBytes();
        RawBytes += item.GetRawBytes();
        AFL_VERIFY(RecordsCount == item.GetRecordsCount())("self_count", RecordsCount)("new_count", item.GetRecordsCount());
    }

};

class TColumnSerializationStat: public TSimpleSerializationStat {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(std::string, ColumnName);
public:
    TColumnSerializationStat(const ui32 columnId, const std::string& columnName)
        : ColumnId(columnId)
        , ColumnName(columnName) {

    }

    TColumnSerializationStat RecalcForRecordsCount(const ui64 recordsCount) const {
        TColumnSerializationStat result(ColumnId, ColumnName);
        result.Merge(TSimpleSerializationStat(SerializedBytes / RecordsCount * recordsCount, recordsCount, RawBytes / RecordsCount * recordsCount));
        return result;
    }

    void Merge(const TSimpleSerializationStat& item) {
        SerializedBytes += item.GetSerializedBytes();
        RawBytes += item.GetRawBytes();
        RecordsCount += item.GetRecordsCount();
    }
};

class TSerializationStats {
private:
    std::deque<TColumnSerializationStat> ColumnStat;
    std::map<ui32, TColumnSerializationStat*> StatsByColumnId;
    std::map<std::string, TColumnSerializationStat*> StatsByColumnName;
public:
    void Merge(const TSerializationStats& item) {
        for (auto&& i : item.ColumnStat) {
            AddStat(i);
        }
    }

    void AddStat(const TColumnSerializationStat& info) {
        auto it = StatsByColumnId.find(info.GetColumnId());
        if (it == StatsByColumnId.end()) {
            ColumnStat.emplace_back(info);
            AFL_VERIFY(StatsByColumnId.emplace(info.GetColumnId(), &ColumnStat.back()).second)("column_id", info.GetColumnId())("column_name", info.GetColumnName());
            AFL_VERIFY(StatsByColumnName.emplace(info.GetColumnName(), &ColumnStat.back()).second)("column_id", info.GetColumnId())("column_name", info.GetColumnName());
        } else {
            it->second->Merge(info);
        }
    }

    std::optional<TColumnSerializationStat> GetColumnInfo(const ui32 columnId) const {
        auto it = StatsByColumnId.find(columnId);
        if (it == StatsByColumnId.end()) {
            return {};
        } else {
            return *it->second;
        }
    }

    std::optional<TColumnSerializationStat> GetColumnInfo(const std::string& columnName) const {
        auto it = StatsByColumnName.find(columnName);
        if (it == StatsByColumnName.end()) {
            return {};
        } else {
            return *it->second;
        }
    }

    std::optional<TBatchSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::RecordBatch>& rb) const;
    std::optional<TBatchSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::Schema>& schema) const;
};

}
