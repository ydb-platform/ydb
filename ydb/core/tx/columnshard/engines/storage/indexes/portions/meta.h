#pragma once
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/saver.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>

namespace NKikimr::NOlap::NIndexes {

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
    virtual void DoAddInplaceIntoPortion(TPortionInfoConstructor& portionInfo) const override;

public:
    TPortionIndexChunk(const TChunkAddress& address, const ui32 recordsCount, const ui64 rawBytes, const TString& data)
        : TBase(address.GetColumnId(), address.GetChunkIdx())
        , RecordsCount(recordsCount)
        , RawBytes(rawBytes)
        , Data(data)
    {
    }

};

class TIndexByColumns: public IIndexMeta {
private:
    using TBase = IIndexMeta;
    std::shared_ptr<NArrow::NSerialization::ISerializer> Serializer;
protected:
    std::set<ui32> ColumnIds;
    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader) const = 0;

    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const override final;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;

    TConclusionStatus CheckSameColumnsForModification(const IIndexMeta& newMeta) const;

public:
    TIndexByColumns() = default;
    TIndexByColumns(const ui32 indexId, const TString& indexName, const std::set<ui32>& columnIds, const TString& storageId);
};

}   // namespace NKikimr::NOlap::NIndexes