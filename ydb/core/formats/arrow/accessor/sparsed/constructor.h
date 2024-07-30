#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NSparsed {

class TConstructor: public IConstructor {
public:
    static TString GetClassNameStatic() {
        return TGlobalConst::SparsedDataAccessorName;
    }

private:
    static inline auto Registrator = TFactory::TRegistrator<TConstructor>(GetClassNameStatic());
    virtual TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> DoConstruct(
        const std::shared_ptr<arrow::RecordBatch>& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override;
    virtual std::shared_ptr<arrow::Schema> DoGetExpectedSchema(const std::shared_ptr<arrow::Field>& resultColumn) const override;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSparsed
