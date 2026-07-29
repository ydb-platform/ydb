#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>

// Sub-column serialization variants that densely re-encode binary lengths and dictionary indexes.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TBinaryDenseConstructor: public IConstructor {
private:
    NPlain::TConstructor Base;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual TBlobWithAdditionalAccessorData DoSerializeToBlobAndMeta(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const override {
        return Base.Construct(originalArray, externalInfo);
    }
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override {
        return Base.ConstructDefault(externalInfo);
    }
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override {
        return Base.SerializeToProto();
    }
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override {
        return Base.DeserializeFromProto(proto);
    }
    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& /*item*/) const override {
        return true;
    }

public:
    TBinaryDenseConstructor()
        : IConstructor(IChunkedArray::EType::Array) {
    }

    virtual TString GetClassName() const override {
        return "SUB_COLUMNS_BINARY_DENSE";
    }
};

class TDictionaryDenseConstructor: public IConstructor {
private:
    NDictionary::TConstructor Base;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual TBlobWithAdditionalAccessorData DoSerializeToBlobAndMeta(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const override {
        return Base.Construct(originalArray, externalInfo);
    }
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override {
        return Base.ConstructDefault(externalInfo);
    }
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override {
        return Base.SerializeToProto();
    }
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override {
        return Base.DeserializeFromProto(proto);
    }
    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& /*item*/) const override {
        return true;
    }

public:
    TDictionaryDenseConstructor()
        : IConstructor(IChunkedArray::EType::Dictionary) {
    }

    virtual TString GetClassName() const override {
        return "SUB_COLUMNS_DICT_DENSE";
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
