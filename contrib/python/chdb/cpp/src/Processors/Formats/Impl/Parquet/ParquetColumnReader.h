#pragma once

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>

namespace parquet20
{

class PageReader;
class ColumnChunkMetaData;
class DataPageV1;
class DataPageV2;

}

namespace DB_CHDB
{

class ParquetColumnReader
{
public:
    virtual ColumnWithTypeAndName readBatch(UInt64 rows_num, const String & name) = 0;

    virtual ~ParquetColumnReader() = default;
};

using ParquetColReaderPtr = std::unique_ptr<ParquetColumnReader>;
using ParquetColReaders = std::vector<ParquetColReaderPtr>;

}
