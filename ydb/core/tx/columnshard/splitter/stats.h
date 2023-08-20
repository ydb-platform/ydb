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

class TColumnSerializationStat {
private:
    YDB_READONLY(ui64, SerializedBytes, 0);
    YDB_READONLY(ui64, RecordsCount, 0);
public:
    TColumnSerializationStat(const ui64 bytes, const ui64 recordsCount)
        : SerializedBytes(bytes)
        , RecordsCount(recordsCount)
    {
        Y_VERIFY(RecordsCount);
    }

    TColumnSerializationStat RecalcForRecordsCount(const ui64 recordsCount) const {
        return TColumnSerializationStat(SerializedBytes / RecordsCount * recordsCount, recordsCount);
    }

    void Add(const TColumnSerializationStat& item) {
        SerializedBytes += item.SerializedBytes;
        AFL_VERIFY(RecordsCount == item.RecordsCount)("self_count", RecordsCount)("new_count", item.RecordsCount);
    }

    std::optional<ui64> PredictOptimalPackRecordsCount(const ui64 recordsCount, const ui64 blobSize) const {
        if (!RecordsCount) {
            return {};
        }
        const ui64 fullSize = 1.0 * recordsCount / RecordsCount * SerializedBytes;
        if (fullSize < blobSize) {
            return recordsCount;
        } else {
            return std::floor(1.0 * fullSize / blobSize * recordsCount);
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

class TSerializationStats {
private:
    std::deque<TColumnSerializationStat> ColumnStat;
    std::map<ui32, TColumnSerializationStat*> StatsByColumnId;
    std::map<std::string, TColumnSerializationStat*> StatsByColumnName;
public:
    void AddStat(const ui32 columnId, const std::string& fieldName, const TColumnSerializationStat& info) {
        ColumnStat.emplace_back(info);
        StatsByColumnId.emplace(columnId, &ColumnStat.back());
        StatsByColumnName.emplace(fieldName, &ColumnStat.back());
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

    std::optional<TColumnSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::RecordBatch>& rb) const;
    std::optional<TColumnSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::Schema>& schema) const;
};

}
