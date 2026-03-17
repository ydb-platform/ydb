#pragma once

#include <generated/parquet_types.h> // in contrib/arrow/cpp/src/ , generated from parquet.thrift
#include <IO/WriteBuffer.h>

namespace DB_CHDB::Parquet
{

/// Returns number of bytes written.
template <typename T>
size_t serializeThriftStruct(const T & obj, WriteBuffer & out);

extern template size_t serializeThriftStruct<parquet20::format::PageHeader>(const parquet20::format::PageHeader &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parquet20::format::ColumnChunk>(const parquet20::format::ColumnChunk &, WriteBuffer & out);
extern template size_t serializeThriftStruct<parquet20::format::FileMetaData>(const parquet20::format::FileMetaData &, WriteBuffer & out);

}
