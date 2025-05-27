#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NDictionary {

class TConstructor: public IConstructor {
private:
    using TBase = IConstructor;

public:
    static TString GetClassNameStatic() {
        return TGlobalConst::DictionaryAccessorName;
    }

private:
    static inline auto Registrator = TFactory::TRegistrator<TConstructor>(GetClassNameStatic());

    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& /*item*/) const override {
        return true;
    }

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const override;

    virtual TString DoSerializeToString(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;
    virtual TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override;
    std::shared_ptr<arrow::Schema> GetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override;

public:
    static std::shared_ptr<arrow::DataType> GetTypeByVariantsCount(const ui32 count);

    TConstructor()
        : TBase(IChunkedArray::EType::Dictionary) {
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NDictionary
