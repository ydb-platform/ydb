#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NAccessor {

class IConstructor {
public:
    using TFactory = NObjectFactory::TObjectFactory<IConstructor, TString>;
    using TProto = NKikimrArrowAccessorProto::TConstructor;

private:
    YDB_READONLY(IChunkedArray::EType, Type, IChunkedArray::EType::Undefined);

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const = 0;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const = 0;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) = 0;
    virtual TString DoDebugString() const {
        return "";
    }
    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& item) const = 0;
    virtual TString DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const = 0;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const = 0;

public:
    virtual bool HasInternalConversion() const {
        return false;
    }

    IConstructor(const IChunkedArray::EType type)
        : Type(type) {
    }

    virtual ~IConstructor() = default;

    TString SerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const;

    bool IsEqualWithSameTypeTo(const IConstructor& item) const {
        return DoIsEqualWithSameTypeTo(item);
    }

    TString DebugString() const {
        return TStringBuilder() << GetClassName() << ":" << DoDebugString();
    }

    TConclusion<std::shared_ptr<IChunkedArray>> DeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const {
        return DoDeserializeFromString(originalData, externalInfo);
    }

    TConclusion<std::shared_ptr<IChunkedArray>> ConstructDefault(const TChunkConstructionData& externalInfo) const {
        return DoConstructDefault(externalInfo);
    }

    TConclusion<std::shared_ptr<IChunkedArray>> Construct(
        const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const {
        AFL_VERIFY(originalArray);
        if (originalArray->GetType() == GetType()) {
            return originalArray;
        } else {
            auto result = DoConstruct(originalArray, externalInfo);
            if (result.IsFail()) {
                return result;
            }
            AFL_VERIFY(result.GetResult()->GetRecordsCount() == originalArray->GetRecordsCount())("result", result.GetResult()->GetRecordsCount())(
                                                      "original", originalArray->GetRecordsCount());
            return result;
        }
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

    TString SerializeToString(const std::shared_ptr<IChunkedArray>& batch, const TChunkConstructionData& externalInfo) const {
        AFL_VERIFY(!!GetObjectPtr());
        return GetObjectPtr()->SerializeToString(batch, externalInfo);
    }

    TConclusion<std::shared_ptr<IChunkedArray>> DeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const {
        AFL_VERIFY(!!GetObjectPtr());
        return GetObjectPtr()->DeserializeFromString(originalData, externalInfo);
    }

    static TConstructorContainer GetDefaultConstructor();
};

}   // namespace NKikimr::NArrow::NAccessor
