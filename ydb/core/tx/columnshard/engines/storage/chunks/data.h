#pragma once
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NChunks {

class TPortionIndexChunk: public IPortionDataChunk {
private:
    using TBase = IPortionDataChunk;
    const ui32 RecordsCount;
    const ui64 RawBytes;
    const TString Data;
protected:
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplit(const TColumnSaver& /*saver*/, const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/, const std::vector<ui64>& /*splitSizes*/) const override {
        AFL_VERIFY(false);
        return {};
    }
    virtual bool DoIsSplittable() const override {
        return false;
    }
    virtual std::optional<ui32> DoGetRecordsCount() const override {
        return RecordsCount;
    }
    virtual std::optional<ui64> DoGetRawBytes() const override {
        return RawBytes;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return nullptr;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return nullptr;
    }
    virtual void DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfoConstructor& portionInfo) const override;
    virtual std::shared_ptr<IPortionDataChunk> DoCopyWithAnotherBlob(TString&& data, const TSimpleColumnInfo& /*columnInfo*/) const override {
        return std::make_shared<TPortionIndexChunk>(GetChunkAddressVerified(), RecordsCount, RawBytes, std::move(data));
    }
public:
    TPortionIndexChunk(const TChunkAddress& address, const ui32 recordsCount, const ui64 rawBytes, const TString& data)
        : TBase(address.GetColumnId(), address.GetChunkIdx())
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
        , Data(data)
    {
    }

};
}   // namespace NKikimr::NOlap::NChunks