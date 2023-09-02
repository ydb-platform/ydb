#pragma once
#include "stats.h"

#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class ISchemaDetailInfo {
public:
    using TPtr = std::shared_ptr<ISchemaDetailInfo>;
    virtual ~ISchemaDetailInfo() = default;
    virtual ui32 GetColumnId(const std::string& fieldName) const = 0;
    virtual TColumnSaver GetColumnSaver(const ui32 columnId) const = 0;
    virtual std::shared_ptr<arrow::Field> GetField(const ui32 columnId) const = 0;
    virtual std::optional<TColumnSerializationStat> GetColumnSerializationStats(const ui32 columnId) const = 0;
    virtual bool NeedMinMaxForColumn(const ui32 columnId) const = 0;
    virtual bool IsSortedColumn(const ui32 columnId) const = 0;
    virtual std::optional<TBatchSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& rb) const = 0;
};
}
