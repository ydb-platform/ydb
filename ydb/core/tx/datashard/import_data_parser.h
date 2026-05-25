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
    using TAddRowFn = std::function<void(const TVector<TCell>& keys, const TVector<TCell>& values)>;

    struct TParsedData {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
    };

    virtual ~IDataParser() = default;

    virtual std::expected<void, TString> Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme) = 0;

    virtual std::expected<TParsedData, TString> ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        const TAddRowFn& addRow) = 0;
};

class IParquetStreamParser {
public:
    struct TParsedBatch {
        ui64 DataBytes = 0;
        ui64 Rows = 0;
        bool HasMore = false;
    };

    virtual ~IParquetStreamParser() = default;

    virtual bool HasOpenFile() const = 0;

    virtual std::expected<void, TString> OpenFile(TStringBuf data) = 0;

    virtual std::expected<void, TString> OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    // Opens and validates file metadata without creating a record-batch reader.
    // The source may be populated with one row group's bytes at a time later.
    virtual std::expected<void, TString> OpenMetadata(
        std::shared_ptr<arrow::io::RandomAccessFile> source) = 0;

    virtual std::expected<void, TString> OpenRowGroup(ui32 rowGroupIndex) = 0;

    virtual void ResetRowGroup() = 0;

    virtual std::expected<TParsedBatch, TString> ProcessNextBatch(
        TMemoryPool& pool,
        const IDataParser::TAddRowFn& addRow) = 0;

    virtual void ResetFile() = 0;
};

IParquetStreamParser* AsParquetStreamParser(IDataParser* parser);

IDataParser::TPtr CreateCsvDataParser();
IDataParser::TPtr CreateParquetDataParser();

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
