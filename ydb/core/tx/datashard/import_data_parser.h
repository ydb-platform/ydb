#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "import_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

#include <expected>
#include <memory>

namespace arrow {
class MemoryPool;
}

namespace parquet {
class FileMetaData;
}

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NKikimr::NDataShard {

class IDataParser {
public:
    using TPtr = THolder<IDataParser>;
    // Takes a row; the cells are valid only during the call. An error rejects the row,
    // and the parser adds the row's place in the file and stops.
    using TAddRowFn = std::function<std::expected<void, TString>(
        const TVector<TCell>& keys, const TVector<TCell>& values)>;

    struct TParsedData {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
    };

    virtual ~IDataParser() = default;

    // Binds the parser to the table: columns, key and types come from the backup's scheme.
    virtual std::expected<void, TString> Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme) = 0;

    // Parses one block, calling addRow for every row until one is rejected.
    virtual std::expected<TParsedData, TString> ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        const TAddRowFn& addRow) = 0;
};

// Parquet is not a byte stream: the footer is read first, then the row groups one at
// a time from a random-access source.
class IParquetStreamParser : public IDataParser {
public:
    using TPtr = THolder<IParquetStreamParser>;

    struct TParsedBatch {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
        bool HasMore = false;
    };

    struct TRowGroupInfo {
        // Uncompressed page bytes of the table's columns, by the footer. The decoded rows
        // can take more.
        ui64 UncompressedBytes = 0;
    };

    virtual bool HasOpenFile() const = 0;

    virtual std::expected<void, TString> OpenFile(TStringBuf data) = 0;

    virtual std::expected<void, TString> OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    // Opens the metadata and checks the schema; the source may get the row groups' bytes later.
    virtual std::expected<void, TString> OpenMetadata(
        std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    // The leaf columns the table reads; the other columns are neither decoded nor downloaded.
    virtual const std::vector<int>& GetColumnIndices() const = 0;

    // The row groups of the file whose metadata is open.
    virtual TVector<TRowGroupInfo> GetRowGroups() const = 0;

    // The metadata of the open file, parsed once.
    virtual std::shared_ptr<parquet::FileMetaData> GetFileMetadata() const = 0;

    // What the parsed footer takes in memory, by the walk over it before it was parsed.
    virtual ui64 GetFooterMemoryEstimate() const = 0;

    // The pool the decoding takes its memory from, with its limit.
    virtual arrow::MemoryPool* GetMemoryPool() = 0;

    virtual std::expected<void, TString> OpenRowGroup(ui32 rowGroupIndex) = 0;

    virtual void ResetRowGroup() = 0;

    // Decodes rows until they take maxDataBytes in an upload (0 = no limit) or the row group ends.
    virtual std::expected<TParsedBatch, TString> ProcessNextBatch(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        ui64 maxDataBytes) = 0;

    virtual void ResetFile() = 0;
};

IDataParser::TPtr CreateCsvDataParser();
// bufferSizeLimit: the read buffer limit of the import; decoding may take twice as much.
// 0 = no limit.
IParquetStreamParser::TPtr CreateParquetDataParser(ui64 bufferSizeLimit);

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
