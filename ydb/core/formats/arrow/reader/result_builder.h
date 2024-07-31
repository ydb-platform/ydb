#pragma once
#include "position.h"
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/types.h>
#include <optional>

namespace NKikimr::NArrow::NMerger {

class TRecordBatchBuilder {
private:
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_ACCESSOR_DEF(std::optional<ui32>, MemoryBufferLimit);

    ui64 CurrentBytesUsed = 0;
    bool IsSameFieldsSequence(const std::vector<std::shared_ptr<arrow::Field>>& f1, const std::vector<std::shared_ptr<arrow::Field>>& f2);

public:

    class TRecordGuard {
    private:
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& Builders;
        ui32 ColumnIdx = 0;
    public:
        TRecordGuard(std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders)
            : Builders(builders)
        {

        }

        ~TRecordGuard() {
            AFL_VERIFY(ColumnIdx == Builders.size());
        }

        void Add(const arrow::Array& arr, const ui32 position, ui64* recordSize = nullptr) {
            AFL_VERIFY(ColumnIdx < Builders.size());
            AFL_VERIFY(NArrow::Append(*Builders[ColumnIdx], arr, position, recordSize));
            AFL_VERIFY(++ColumnIdx <= Builders.size());
        }
    };

    TRecordGuard StartRecord() {
        return TRecordGuard(Builders);
    }

    ui32 GetBuildersCount() const {
        return Builders.size();
    }

    TString GetColumnNames() const;

    TRecordBatchBuilder(const std::vector<std::shared_ptr<arrow::Field>>& fields, const std::optional<ui32> rowsCountExpectation = {}, const THashMap<std::string, ui64>& fieldDataSizePreallocated = {});

    std::shared_ptr<arrow::RecordBatch> Finalize();

    bool IsBufferExhausted() const {
        return MemoryBufferLimit && *MemoryBufferLimit < CurrentBytesUsed;
    }
    void AddRecord(const TCursor& position);
    void AddRecord(const TRWSortableBatchPosition& position);
    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema);
};

}
