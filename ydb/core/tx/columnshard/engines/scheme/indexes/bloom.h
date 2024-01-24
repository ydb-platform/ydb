#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <library/cpp/object_factory/object_factory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NKikimr::NOlap::NIndexes {

class TBloomIndexConstructor: public IIndexMetaConstructor {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }
private:
    std::set<TString> ColumnNames;
    double FalsePositiveProbability = 0.1;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexConstructor>(GetClassNameStatic());
protected:
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const override;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const override;

public:
    TBloomIndexConstructor() = default;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

class TBloomIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }
private:
    using TBase = TIndexByColumns;
    std::shared_ptr<arrow::Schema> ResultSchema;
    const ui64 RowsCountExpectation = 10000;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    ui32 BitsCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability > 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
        BitsCount = RowsCountExpectation * HashesCount / (std::log(2));
    }
protected:
    virtual std::shared_ptr<IIndexChecker> DoBuildIndexChecker(const TProgramContainer& /*program*/) const override {
        return nullptr;
    }

    virtual std::shared_ptr<arrow::RecordBatch> DoBuildIndexImpl(TChunkedBatchReader& reader) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(proto.HasBloomFilter());
        auto& bFilter = proto.GetBloomFilter();
        FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
        for (auto&& i : bFilter.GetColumnIds()) {
            ColumnIds.emplace(i);
        }
        Initialize();
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomFilter();
        filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
        for (auto&& i : ColumnIds) {
            filterProto->AddColumnIds(i);
        }
    }

public:
    TBloomIndexMeta() = default;
    TBloomIndexMeta(const std::set<ui32>& columnIds, const double fpProbability)
        : TBase(columnIds)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes