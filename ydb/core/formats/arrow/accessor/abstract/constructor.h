#pragma once

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/chunk_data.h>
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
    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& item) const = 0;
    virtual std::shared_ptr<arrow::RecordBatch> DoConstruct(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const = 0;

public:
    virtual ~IConstructor() = default;

    std::shared_ptr<arrow::RecordBatch> Construct(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
        AFL_VERIFY(columnData);
        return DoConstruct(columnData, externalInfo);
    }

    bool IsEqualWithSameTypeTo(const IConstructor& item) const {
        return DoIsEqualWithSameTypeTo(item);
    }

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

    bool IsEqualTo(const TConstructorContainer& item) const {
        if (!GetObjectPtr() && !item.GetObjectPtr()) {
            return true;
        } else if (!!GetObjectPtr() && !!item.GetObjectPtr()) {
            if (GetObjectPtr()->GetClassName() != item.GetObjectPtr()->GetClassName()) {
                return false;
            }
            return GetObjectPtr()->IsEqualWithSameTypeTo(*item.GetObjectPtr());
        } else {
            return false;
        }
    }

    std::shared_ptr<arrow::RecordBatch> Construct(const std::shared_ptr<IChunkedArray>& batch, const TChunkConstructionData& externalInfo) const {
        AFL_VERIFY(!!GetObjectPtr());
        return GetObjectPtr()->Construct(batch, externalInfo);
    }

    static TConstructorContainer GetDefaultConstructor();
};

}   // namespace NKikimr::NArrow::NAccessor
