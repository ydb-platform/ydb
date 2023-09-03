#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/splitter/stats.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnMergeContext {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    ISnapshotSchema::TPtr SchemaInfo;
    YDB_READONLY_DEF(TColumnSaver, Saver);
    YDB_READONLY_DEF(std::shared_ptr<TColumnLoader>, Loader);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, Field);
    YDB_READONLY(ui32, PortionRowsCountLimit, 10000);
    YDB_READONLY(ui64, ChunkPackedBytesLimit, 7 * 1024 * 1024);
    YDB_READONLY(ui64, ExpectedBlobPackedBytes, 4 * 1024 * 1024);
    YDB_READONLY(ui64, ChunkRawBytesLimit, 50 * 1024 * 1024);
    YDB_READONLY(ui64, StorePackedChunkSizeLimit, 512 * 1024);

    TColumnSerializationStat ColumnStat;
    const TIndexInfo& IndexInfo;
public:
    ISnapshotSchema::TPtr GetSchemaInfo() const {
        return SchemaInfo;
    }

    const TColumnSerializationStat& GetColumnStat() const {
        return ColumnStat;
    }

    std::unique_ptr<arrow::ArrayBuilder> MakeBuilder() const {
        return NArrow::MakeBuilder(Field);
    }

    const TIndexInfo& GetIndexInfo() const {
        return IndexInfo;
    }

    TColumnMergeContext(const ISnapshotSchema::TPtr& schema, const ui32 portionRowsCountLimit, const ui32 chunkRawBytesLimit,
        std::shared_ptr<arrow::Field> f, const TColumnSerializationStat& columnStat, const TSaverContext& saverContext)
        : ColumnId(schema->GetColumnId(f->name()))
        , SchemaInfo(schema)
        , Saver(schema->GetColumnSaver(schema->GetColumnId(f->name()), saverContext))
        , Loader(schema->GetColumnLoader(schema->GetColumnId(f->name())))
        , Field(f)
        , PortionRowsCountLimit(portionRowsCountLimit)
        , ChunkRawBytesLimit(chunkRawBytesLimit)
        , ColumnStat(columnStat)
        , IndexInfo(schema->GetIndexInfo()) {
        Y_VERIFY(PortionRowsCountLimit);
        Y_VERIFY(ChunkRawBytesLimit);
    }
};

}
