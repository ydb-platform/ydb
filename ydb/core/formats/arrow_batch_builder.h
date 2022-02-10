#pragma once
#include "arrow_helpers.h"
#include <ydb/core/formats/factory.h> 
#include <ydb/core/scheme/scheme_tablecell.h> 

namespace NKikimr::NArrow {

/// YDB rows to arrow::RecordBatch converter
class TArrowBatchBuilder : public NKikimr::IBlockBuilder {
public:
    static constexpr const size_t DEFAULT_ROWS_TO_RESERVE = 1000;

    /// @note compression is disabled by default KIKIMR-11690
    // Allowed codecs: UNCOMPRESSED, LZ4_FRAME, ZSTD
    TArrowBatchBuilder(arrow::Compression::type codec = arrow::Compression::UNCOMPRESSED);
    ~TArrowBatchBuilder() = default;

    bool Start(const TVector<std::pair<TString, NScheme::TTypeId>>& columns,
               ui64 maxRowsInBlock, ui64 maxBytesInBlock, TString& err) override {
        Y_UNUSED(maxRowsInBlock);
        Y_UNUSED(maxBytesInBlock);
        Y_UNUSED(err);
        return Start(columns);
    }

    void AddRow(const NKikimr::TDbTupleRef& key, const NKikimr::TDbTupleRef& value) override;
    void AddRow(const TConstArrayRef<TCell>& key, const TConstArrayRef<TCell>& value);

    // You have to call it before Start()
    void Reserve(size_t numRows) {
        RowsToReserve = numRows;
    }

    void ReserveData(ui32 column, size_t size);
    TString Finish() override;

    size_t Bytes() const override {
        return NumBytes;
    }

    bool Start(const TVector<std::pair<TString, NScheme::TTypeId>>& columns);
    std::shared_ptr<arrow::RecordBatch> FlushBatch(bool reinitialize);
    std::shared_ptr<arrow::RecordBatch> GetBatch() const { return Batch; }

protected:
    void AppendCell(const TCell& cell, ui32 colNum);

    const TVector<std::pair<TString, NScheme::TTypeId>>& GetYdbSchema() const {
        return YdbSchema;
    }

private:
    arrow::ipc::IpcWriteOptions WriteOptions;
    TVector<std::pair<TString, NScheme::TTypeId>> YdbSchema;
    std::unique_ptr<arrow::RecordBatchBuilder> BatchBuilder;
    std::shared_ptr<arrow::RecordBatch> Batch;
    size_t RowsToReserve{DEFAULT_ROWS_TO_RESERVE};

protected:
    size_t NumRows{0};
    size_t NumBytes{0};

private:
    std::unique_ptr<IBlockBuilder> Clone() const override {
        return std::make_unique<TArrowBatchBuilder>();
    }
};

// Creates a batch with single column of type NullType and with num_rows equal rowsCount. All values are null. We need
// this function, because batch can not have zero columns. And NullType conusumes the least place in memory.
std::shared_ptr<arrow::RecordBatch> CreateNoColumnsBatch(ui64 rowsCount);

}
