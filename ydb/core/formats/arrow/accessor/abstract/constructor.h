#pragma once
#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/protos/accessor.pb.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NAccessor {

class IConstructor {
public:
    using TFactory = NObjectFactory::TObjectFactory<IConstructor, TString>;
    using TProto = NKikimrArrowAccessorProto::TConstructor;

private:
    virtual TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> DoConstruct(
        const std::shared_ptr<arrow::RecordBatch>& originalData, const TChunkConstructionData& externalInfo) const = 0;
    virtual TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> DoConstructDefault(
        const TChunkConstructionData& externalInfo) const = 0;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) = 0;
    virtual std::shared_ptr<arrow::Schema> DoGetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const = 0;
    virtual TString DoDebugString() const {
        return "";
    }

public:
    virtual ~IConstructor() = default;

    TString DebugString() const {
        return TStringBuilder() << GetClassName() << ":" << DoDebugString();
    }

    TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Construct(
        const std::shared_ptr<arrow::RecordBatch>& originalData, const TChunkConstructionData& externalInfo) const {
        return DoConstruct(originalData, externalInfo);
    }

    TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> ConstructDefault(const TChunkConstructionData& externalInfo) const {
        return DoConstructDefault(externalInfo);
    }

    bool DeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) {
        return DoDeserializeFromProto(proto);
    }

    NKikimrArrowAccessorProto::TConstructor SerializeToProto() const {
        return DoSerializeToProto();
    }

    void SerializeToProto(NKikimrArrowAccessorProto::TConstructor& proto) const {
        proto = DoSerializeToProto();
    }

    std::shared_ptr<arrow::Schema> GetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const {
        AFL_VERIFY(resultColumn);
        return DoGetExpectedSchema(resultColumn);
    }

    virtual TString GetClassName() const = 0;
};

class TConstructorContainer: public NBackgroundTasks::TInterfaceProtoContainer<IConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IConstructor>;

public:
    using TBase::TBase;

    static TConstructorContainer GetDefaultConstructor();
};

}   // namespace NKikimr::NArrow::NAccessor
