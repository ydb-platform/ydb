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

    bool IsSameFieldsSequence(const std::vector<std::shared_ptr<arrow::Field>>& f1, const std::vector<std::shared_ptr<arrow::Field>>& f2);

public:
    ui32 GetBuildersCount() const {
        return Builders.size();
    }

    TString GetColumnNames() const;

    TRecordBatchBuilder(const std::vector<std::shared_ptr<arrow::Field>>& fields, const std::optional<ui32> rowsCountExpectation = {}, const THashMap<std::string, ui64>& fieldDataSizePreallocated = {});

    std::shared_ptr<arrow::RecordBatch> Finalize();

    void AddRecord(const TSortableBatchPosition& position);
    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& schema);
};

}
