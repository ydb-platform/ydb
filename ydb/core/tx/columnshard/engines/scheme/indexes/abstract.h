#pragma once

#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <library/cpp/object_factory/object_factory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NKikimr::NOlap {
struct TIndexInfo;
}

namespace NKikimr::NOlap::NIndexes {

class IIndexChecker {
protected:
    virtual bool DoCheck(std::vector<TString>&& blobs) const = 0;
public:
    virtual ~IIndexChecker() = default;
    bool Check(std::vector<TString>&& blobs) const {
        return DoCheck(std::move(blobs));
    }
};

class TIndexCheckerContainer {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY_DEF(std::shared_ptr<IIndexChecker>, Object);
public:
    TIndexCheckerContainer(const ui32 indexId, const std::shared_ptr<IIndexChecker>& object)
        : IndexId(indexId)
        , Object(object) {
        AFL_VERIFY(IndexId);
        AFL_VERIFY(Object);
    }

    const IIndexChecker* operator->() const {
        return Object.get();
    }
};

class IIndexMeta {
protected:
    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const = 0;
    virtual std::shared_ptr<IIndexChecker> DoBuildIndexChecker(const TProgramContainer& program) const = 0;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMeta, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexDescription;

    virtual ~IIndexMeta() = default;

    std::shared_ptr<IPortionDataChunk> BuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
        return DoBuildIndex(indexId, data, indexInfo);
    }

    std::shared_ptr<IIndexChecker> BuildIndexChecker(const TProgramContainer& program) const {
        return DoBuildIndexChecker(program);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
        return DoSerializeToProto(proto);
    }

    virtual TString GetClassName() const = 0;
};

class TIndexMetaContainer {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY_DEF(std::shared_ptr<IIndexMeta>, Object);
public:
    TIndexMetaContainer(const ui32 indexId, const std::shared_ptr<IIndexMeta>& object)
        : IndexId(indexId)
        , Object(object)
    {
        AFL_VERIFY(IndexId);
        AFL_VERIFY(Object);
    }

    const IIndexMeta* operator->() const {
        return Object.get();
    }

    std::optional<TIndexCheckerContainer> BuildIndexChecker(const TProgramContainer& program) const {
        auto checker = Object->BuildIndexChecker(program);
        if (!checker) {
            return {};
        }
        return TIndexCheckerContainer(IndexId, checker);
    }
};

class TPortionIndexChunk: public IPortionDataChunk {
private:
    using TBase = IPortionDataChunk;
    const TString Data;
protected:
    virtual const TString& DoGetData() const override {
        return Data;
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplit(const TColumnSaver& /*saver*/, const std::shared_ptr<NColumnShard::TSplitterCounters>& /*counters*/, const std::vector<ui64>& /*splitSizes*/) const override {
        return {};
    }
    virtual bool DoIsSplittable() const override {
        return false;
    }
    virtual std::optional<ui32> DoGetRecordsCount() const override {
        return {};
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return nullptr;
    }
    virtual std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return nullptr;
    }
    virtual void DoAddIntoPortion(const TBlobRange& bRange, TPortionInfo& portionInfo) const override;
public:
    TPortionIndexChunk(const ui32 entityId, const TString& data)
        : TBase(entityId, 0) 
        , Data(data)
    {
    }

};

class TIndexByColumns: public IIndexMeta {
private:
    std::vector<ui32> ColumnIds;
    std::shared_ptr<TColumnSaver> Saver;
protected:
    virtual std::shared_ptr<arrow::RecordBatch> DoBuildIndexImpl(TChunkedBatchReader& reader) const = 0;

    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const override final;
public:
};

class TBloomIndexConstructor: public TIndexByColumns {
private:
    std::vector<ui32> ColumnIds;
    std::shared_ptr<arrow::Schema> ResultSchema;
    const ui64 RowsCountExpectation = 10000;
    const double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    ui32 BitsCount = 0;

protected:
    virtual std::shared_ptr<arrow::RecordBatch> DoBuildIndexImpl(TChunkedBatchReader& reader) const override;

public:
    TBloomIndexConstructor(const std::vector<ui32>& columnIds, const double fpProbability)
        : ColumnIds(columnIds)
        , FalsePositiveProbability(fpProbability) {
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability > 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
        BitsCount = RowsCountExpectation * HashesCount / (std::log(2));
    }
};

}   // namespace NKikimr::NOlap::NIndexes