#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>

#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NSparsed {

class TConstructor: public IConstructor {
private:
    using TBase = IConstructor;
public:
    static TString GetClassNameStatic() {
        return TGlobalConst::SparsedDataAccessorName;
    }

private:
    static inline auto Registrator = TFactory::TRegistrator<TConstructor>(GetClassNameStatic());
    std::shared_ptr<arrow::Schema> GetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const;

    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const override;
    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& /*item*/) const override {
        return true;
    }
    virtual TString DoSerializeToString(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;

    virtual TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TConstructor()
        : TBase(IChunkedArray::EType::SparsedArray) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSparsed
