#pragma once
#include "view.h"

#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TRowsCollection {
private:
    std::vector<TString> RawData;

    void Initialize(const std::shared_ptr<arrow::RecordBatch>& data);

public:
    void Reallocate() {
        std::vector<TString> rows;
        for (auto&& r : RawData) {
            rows.emplace_back(r.data(), r.size());
        }
        RawData = std::move(rows);
    }

    ui32 GetMemorySize() const {
        ui32 result = 0;
        for (auto&& i : RawData) {
            result += i.capacity() + sizeof(TString);
        }
        return result;
    }

    ui32 GetDataSize() const {
        ui32 result = 0;
        for (auto&& i : RawData) {
            result += i.size();
        }
        return result;
    }

    ui32 GetRecordsCount() const {
        return RawData.size();
    }

    TSimpleRow GetRecord(const ui32 recordIndex, const std::shared_ptr<arrow::Schema>& schema) const;

    TRowsCollection() = default;

    TRowsCollection(const std::vector<TString>& strings)
        : RawData(strings) {
    }

    TRowsCollection(const std::shared_ptr<arrow::RecordBatch>& data) {
        Initialize(data);
    }

    TSimpleRow GetFirst(const std::shared_ptr<arrow::Schema>& schema) const;

    TSimpleRow GetLast(const std::shared_ptr<arrow::Schema>& schema) const;

    TConclusion<std::shared_ptr<arrow::RecordBatch>> BuildBatch(const std::shared_ptr<arrow::Schema>& schema) const;

    TConclusion<TString> DebugString(const std::shared_ptr<arrow::Schema>& schema) const;
};

}   // namespace NKikimr::NArrow
