#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReader.h>

#include <arrow/io/interfaces.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include "ParquetColumnReader.h"

namespace DB_CHDB
{

class ParquetRecordReader
{
public:
    ParquetRecordReader(
        Block header_,
        parquet20::ArrowReaderProperties arrow_properties_,
        parquet20::ReaderProperties reader_properties_,
        std::shared_ptr<::arrow20::io::RandomAccessFile> arrow_file,
        const FormatSettings & format_settings,
        std::vector<int> row_groups_indices_,
        std::shared_ptr<parquet20::FileMetaData> metadata = nullptr);

    Chunk readChunk();

private:
    std::unique_ptr<parquet20::ParquetFileReader> file_reader;
    parquet20::ArrowReaderProperties arrow_properties;

    Block header;

    std::shared_ptr<parquet20::RowGroupReader> cur_row_group_reader;
    ParquetColReaders column_readers;

    UInt64 max_block_size;

    std::vector<int> parquet_col_indice;
    std::vector<int> row_groups_indices;
    UInt64 left_rows;
    UInt64 cur_row_group_left_rows = 0;
    int next_row_group_idx = 0;

    CHDBPoco::Logger * log;

    void loadNextRowGroup();
    Int64 getTotalRows(const parquet20::FileMetaData & meta_data);
};

}
