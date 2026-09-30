#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "import_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

#include <expected>
#include <memory>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NKikimr::NDataShard {

class IDataParser {
public:
    using TPtr = THolder<IDataParser>;
    // Takes a row of the file. The cells are borrowed and valid only for the
    // duration of the call. An error rejects the row: the one that takes rows
    // knows the table, the parser knows where the row is in the file, so the
    // parser adds the place to the error and stops.
    using TAddRowFn = std::function<std::expected<void, TString>(
        const TVector<TCell>& keys, const TVector<TCell>& values)>;

    struct TParsedData {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
    };

    virtual ~IDataParser() = default;

    // Binds the parser to the destination table. For every format the column
    // order, key positions and types come from the backup's table description
    // (scheme.pb); the data file is only checked against it.
    virtual std::expected<void, TString> Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme) = 0;

    // Parses one self-contained block and calls addRow for every row, until
    // one of them is rejected.
    virtual std::expected<TParsedData, TString> ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        const TAddRowFn& addRow) = 0;
};

// Parquet cannot be consumed as a byte stream: the footer is read first and
// row groups are then decoded one at a time from a random-access source. This
// extends IDataParser with that lifecycle; ParseBlock remains available for a
// file that is already fully in memory.
class IParquetStreamParser : public IDataParser {
public:
    using TPtr = THolder<IParquetStreamParser>;

    struct TParsedBatch {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
        bool HasMore = false;
    };

    struct TRowGroupInfo {
        // The bytes of the pages of the table's columns when they are
        // uncompressed, as the footer of the file states them. The rows
        // decoded from the pages can take more: a page of a dictionary holds a
        // value once, however many rows have it.
        ui64 UncompressedBytes = 0;
    };

    virtual bool HasOpenFile() const = 0;

    virtual std::expected<void, TString> OpenFile(TStringBuf data) = 0;

    virtual std::expected<void, TString> OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    // Opens the file metadata and validates the schema (column names and Arrow
    // types) without creating a record-batch reader. The source may be
    // populated with one row group's bytes at a time later.
    virtual std::expected<void, TString> OpenMetadata(
        std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    // The columns of the file the rows are read from, as the indices of its
    // leaf columns. A column of the file that the table does not have is not
    // among them: it is neither decoded nor downloaded.
    virtual const std::vector<int>& GetColumnIndices() const = 0;

    // The row groups of the file whose metadata is open.
    virtual TVector<TRowGroupInfo> GetRowGroups() const = 0;

    virtual std::expected<void, TString> OpenRowGroup(ui32 rowGroupIndex) = 0;

    virtual void ResetRowGroup() = 0;

    // Decodes rows of the open row group until the rows emitted take
    // maxDataBytes in an upload (0 = no limit) or the row group ends. That is
    // the cell data and a header for every cell, a NULL as well. HasMore
    // reports whether rows remain in the row group.
    virtual std::expected<TParsedBatch, TString> ProcessNextBatch(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        ui64 maxDataBytes) = 0;

    virtual void ResetFile() = 0;
};

IDataParser::TPtr CreateCsvDataParser();
// bufferSizeLimit is the limit of the read buffer of the import, which the
// engine keeps the row groups within. The memory that decoding takes is limited
// by it as well: to twice as much. 0 = no limit.
IParquetStreamParser::TPtr CreateParquetDataParser(ui64 bufferSizeLimit);

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
