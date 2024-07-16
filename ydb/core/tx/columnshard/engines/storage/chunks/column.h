#pragma once
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>

namespace NKikimr::NOlap::NChunks {

class TChunkPreparation: public IPortionColumnChunk {
private:
    using TBase = IPortionColumnChunk;
    TString Data;
    TColumnRecord Record;
    TSimpleColumnInfo ColumnInfo;
    std::shared_ptr<arrow::Scalar> First;
    std::shared_ptr<arrow::Scalar> Last;
protected:
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplitImpl(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const override;
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual ui32 DoGetRecordsCountImpl() const override {
        return Record.GetMeta().GetNumRows();
    }
    virtual ui64 DoGetRawBytesImpl() const override {
        return Record.GetMeta().GetRawBytes();
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual TSimpleChunkMeta DoBuildSimpleChunkMeta() const override {
        return Record.GetMeta();
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return First;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return Last;
    }
    virtual std::shared_ptr<IPortionDataChunk> DoCopyWithAnotherBlob(TString&& data, const TSimpleColumnInfo& columnInfo) const override {
        TColumnRecord cRecord = Record;
        cRecord.ResetBlobRange();
        return std::make_shared<TChunkPreparation>(std::move(data), cRecord, columnInfo);
    }

public:
    const TColumnRecord& GetRecord() const {
        return Record;
    }

    TChunkPreparation(const TString& data, const TColumnRecord& columnChunk, const TSimpleColumnInfo& columnInfo)
        : TBase(columnChunk.ColumnId)
        , Data(data)
        , Record(columnChunk)
        , ColumnInfo(columnInfo) {
        AFL_VERIFY(Data.size() == Record.BlobRange.Size || Record.BlobRange.Size == 0)("data", Data.size())("record", Record.BlobRange.Size);
    }

    TChunkPreparation(const TString& data, const std::shared_ptr<arrow::Array>& column, const TChunkAddress& address, const TSimpleColumnInfo& columnInfo)
        : TBase(address.GetColumnId())
        , Data(data)
        , Record(address, column, columnInfo)
        , ColumnInfo(columnInfo) {
        Y_ABORT_UNLESS(column->length());
        First = NArrow::TStatusValidator::GetValid(column->GetScalar(0));
        Last = NArrow::TStatusValidator::GetValid(column->GetScalar(column->length() - 1));
        Record.BlobRange.Size = data.size();
    }
};

}
