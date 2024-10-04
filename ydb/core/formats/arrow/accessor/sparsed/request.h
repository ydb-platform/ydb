#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/request.h>
#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NSparsed {

class TRequestedConstuctor: public IRequestedConstructor {
public:
    static TString GetClassNameStatic() {
        return TGlobalConst::SparsedDataAccessorName;
    }

private:
    static inline auto Registrator = TFactory::TRegistrator<TRequestedConstuctor>(GetClassNameStatic());
    virtual TConclusion<TConstructorContainer> DoBuildConstructor() const override;
    virtual NKikimrArrowAccessorProto::TRequestedConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TRequestedConstructor& proto) override;
    virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSparsed
