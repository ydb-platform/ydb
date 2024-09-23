#pragma once
#include "arrow_helpers.h"
#include <ydb/core/formats/factory.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NArrow {

class TRecordBatchReader {
private:
    std::shared_ptr<arrow::RecordBatch> Batch;
public:
    TRecordBatchReader() = default;
    TRecordBatchReader(const std::shared_ptr<arrow::RecordBatch>& batch)
        : Batch(batch)
    {
    }
    class TRecordIterator {
    private:
        friend class TRecordBatchReader;
        ui32 RecordIdx = 0;
        const TRecordBatchReader& Reader;
        TRecordIterator(const ui32 recordIdx, const TRecordBatchReader& reader)
            : RecordIdx(recordIdx)
            , Reader(reader) {

        }
    public:
        class TColumnIterator {
        private:
            friend class TRecordIterator;
            ui32 ColumnIdx = 0;
            const ui32 RecordIdx = 0;
            const TRecordBatchReader& Reader;
            TColumnIterator(const ui32 columnIdx, const ui32 recordIdx, const TRecordBatchReader& reader)
                : ColumnIdx(columnIdx)
                , RecordIdx(recordIdx)
                , Reader(reader) {

            }
        public:
            std::shared_ptr<arrow::Scalar> operator*() const {
                auto c = Reader.Batch->column(ColumnIdx);
                if (c->IsNull(RecordIdx)) {
                    return nullptr;
                } else {
                    auto status = c->GetScalar(RecordIdx);
                    Y_ABORT_UNLESS(status.ok());
                    return *status;
                }
            }
            void operator++() {
                ++ColumnIdx;
            }
            bool operator==(const TColumnIterator& value) const {
                return RecordIdx == value.RecordIdx && ColumnIdx == value.ColumnIdx && Reader.Batch.get() == value.Reader.Batch.get();
            }
        };
        TRecordIterator operator*() const {
            return *this;
        }
        bool operator==(const TRecordIterator& value) const {
            return RecordIdx == value.RecordIdx && Reader.Batch.get() == value.Reader.Batch.get();
        }
        void operator++() {
            ++RecordIdx;
        }
        ui32 GetIndex() const {
            return RecordIdx;
        }
        TColumnIterator begin() const {
            return TColumnIterator(0, RecordIdx, Reader);
        }
        TColumnIterator end() const {
            return TColumnIterator(Reader.Batch->schema()->num_fields(), RecordIdx, Reader);
        }
    };
    ui32 GetColumnsCount() const {
        return Batch ? Batch->num_columns() : 0;
    }
    ui32 GetRowsCount() const {
        return Batch ? Batch->num_rows() : 0;
    }
    TRecordIterator begin() const {
        return TRecordIterator(0, *this);
    }
    TRecordIterator end() const {
        return TRecordIterator(GetRowsCount(), *this);
    }
    void SerializeToStrings(TString& schema, TString& data) const;
    bool DeserializeFromStrings(const TString& schemaString, const TString& dataString);
};

class TRecordBatchConstructor {
private:
    ui32 RecordsCount = 0;
    bool InConstruction = false;
    static void AddValueToBuilder(arrow::ArrayBuilder& builder, const std::shared_ptr<arrow::Scalar>& value, const bool withCast);
protected:
    std::shared_ptr<arrow::Schema> Schema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> Builders;
public:
    class TRecordConstructor {
    private:
        TRecordBatchConstructor& Owner;
        const bool WithCast = false;
        std::vector<std::unique_ptr<arrow::ArrayBuilder>>::const_iterator CurrentBuilder;
    public:
        TRecordConstructor(TRecordBatchConstructor& owner, const bool withCast)
            : Owner(owner)
            , WithCast(withCast)
        {
            Y_ABORT_UNLESS(!Owner.InConstruction);
            CurrentBuilder = Owner.Builders.begin();
            Owner.InConstruction = true;
        }
        ~TRecordConstructor() {
            for (; CurrentBuilder != Owner.Builders.end(); ++CurrentBuilder) {
                Y_ABORT_UNLESS((*CurrentBuilder)->AppendNull().ok());
            }
            Owner.InConstruction = false;
            ++Owner.RecordsCount;
        }
        TRecordConstructor& AddRecordValue(const std::shared_ptr<arrow::Scalar>& value);
    };

    TRecordBatchConstructor& InitColumns(const std::shared_ptr<arrow::Schema>& schema);

    TRecordConstructor StartRecord(const bool withCast = false) {
        Y_ABORT_UNLESS(!InConstruction);
        return TRecordConstructor(*this, withCast);
    }

    TRecordBatchConstructor& AddRecordsBatchSlow(const std::shared_ptr<arrow::RecordBatch>& value, const bool withCast = false, const bool withRemap = false);

    void Reserve(const ui32 recordsCount) {
        for (auto&& i : Builders) {
            Y_ABORT_UNLESS(i->Reserve(recordsCount).ok());
        }
    }

    TRecordBatchReader Finish();
};

/// YDB rows to arrow::RecordBatch converter
class TArrowBatchBuilder : public NKikimr::IBlockBuilder {
public:
    static constexpr const size_t DEFAULT_ROWS_TO_RESERVE = 1000;

    /// @note compression is disabled by default KIKIMR-11690
    // Allowed codecs: UNCOMPRESSED, LZ4_FRAME, ZSTD
    TArrowBatchBuilder(arrow::Compression::type codec = arrow::Compression::UNCOMPRESSED, const std::set<std::string>& notNullColumns = {});
    ~TArrowBatchBuilder() = default;

    bool Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns,
               ui64 maxRowsInBlock, ui64 maxBytesInBlock, TString& err) override {
        Y_UNUSED(maxRowsInBlock);
        Y_UNUSED(maxBytesInBlock);
        const auto result = Start(columns);
        if (!result.ok()) {
            err = result.ToString();
        }
        return result.ok();
    }

    void AddRow(const NKikimr::TDbTupleRef& key, const NKikimr::TDbTupleRef& value) override;
    void AddRow(const TConstArrayRef<TCell>& key, const TConstArrayRef<TCell>& value);
    void AddRow(const TConstArrayRef<TCell>& row);

    // You have to call it before Start()
    void Reserve(size_t numRows) {
        RowsToReserve = numRows;
    }

    void ReserveData(ui32 column, size_t size);
    TString Finish() override;

    size_t Bytes() const override {
        return NumBytes;
    }

    arrow::Status Start(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);
    std::shared_ptr<arrow::RecordBatch> FlushBatch(bool reinitialize);
    std::shared_ptr<arrow::RecordBatch> GetBatch() const { return Batch; }

protected:
    void AppendCell(const TCell& cell, ui32 colNum);

    const std::vector<std::pair<TString, NScheme::TTypeInfo>>& GetYdbSchema() const {
        return YdbSchema;
    }

private:
    arrow::ipc::IpcWriteOptions WriteOptions;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    std::unique_ptr<arrow::RecordBatchBuilder> BatchBuilder;
    std::shared_ptr<arrow::RecordBatch> Batch;
    size_t RowsToReserve{DEFAULT_ROWS_TO_RESERVE};
    const std::set<std::string> NotNullColumns;

protected:
    size_t NumRows{0};
    size_t NumBytes{0};

private:
    std::unique_ptr<IBlockBuilder> Clone() const override {
        return std::make_unique<TArrowBatchBuilder>(WriteOptions.codec->compression_type(), NotNullColumns);
    }
};

// Creates a batch with single column of type NullType and with num_rows equal rowsCount. All values are null. We need
// this function, because batch can not have zero columns. And NullType conusumes the least place in memory.
std::shared_ptr<arrow::RecordBatch> CreateNoColumnsBatch(ui64 rowsCount);

}
