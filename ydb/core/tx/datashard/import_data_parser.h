#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "import_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/interfaces.h>

#include <memory>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NKikimr::NDataShard {

class IDataParser {
public:
    using TPtr = THolder<IDataParser>;

    virtual ~IDataParser() = default;

    virtual bool Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme,
        TString& error) = 0;

    using TAddRowFn = std::function<void(const TVector<TCell>& keys, const TVector<TCell>& values)>;

    virtual bool ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        ui64& pendingBytes,
        ui64& pendingRows,
        const TAddRowFn& addRow,
        TString& error) = 0;
};

class IParquetStreamParser {
public:
    virtual ~IParquetStreamParser() = default;

    virtual bool HasOpenFile() const = 0;

    virtual bool OpenFile(TStringBuf data, TString& error) = 0;

    virtual bool OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source, TString& error) = 0;

    virtual bool ProcessNextBatch(
        TMemoryPool& pool,
        ui64& pendingBytes,
        ui64& pendingRows,
        const IDataParser::TAddRowFn& addRow,
        TString& error) = 0;

    virtual void ResetFile() = 0;
};

IParquetStreamParser* AsParquetStreamParser(IDataParser* parser);

IDataParser::TPtr CreateCsvDataParser();
IDataParser::TPtr CreateParquetDataParser();

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
