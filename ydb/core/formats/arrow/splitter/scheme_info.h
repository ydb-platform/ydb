#pragma once
#include "stats.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/save_load/saver.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow::NSplitter {

class ISchemaDetailInfo {
private:
    YDB_ACCESSOR_DEF(std::optional<NSerialization::TSerializerContainer>, OverrideSerializer);

protected:
    virtual NAccessor::TColumnSaver DoGetColumnSaver(const ui32 columnId) const = 0;

public:
    using TPtr = std::shared_ptr<ISchemaDetailInfo>;
    virtual ~ISchemaDetailInfo() = default;
    virtual ui32 GetColumnId(const std::string& fieldName) const = 0;
    NAccessor::TColumnSaver GetColumnSaver(const ui32 columnId) const;
    virtual std::shared_ptr<arrow::Field> GetField(const ui32 columnId) const = 0;
    virtual std::optional<TColumnSerializationStat> GetColumnSerializationStats(const ui32 columnId) const = 0;
    virtual bool NeedMinMaxForColumn(const ui32 columnId) const = 0;
    virtual bool IsSortedColumn(const ui32 columnId) const = 0;
    virtual std::optional<TBatchSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& rb) const = 0;
};
}   // namespace NKikimr::NArrow::NSplitter
