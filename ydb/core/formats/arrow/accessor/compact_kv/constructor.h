#pragma once

#include "settings.h"

#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NCompactKV {

class TConstructor: public IConstructor {
private:
    using TBase = IConstructor;
    TSettings Settings;

public:
    static TString GetClassNameStatic() {
        return TGlobalConst::CompactKVDataAccessorName;
    }
    const TSettings& GetSettings() const {
        return Settings;
    }

private:
    static inline auto Registrator = TFactory::TRegistrator<TConstructor>(GetClassNameStatic());

    virtual bool DoIsEqualWithSameTypeTo(const IConstructor& /*item*/) const override {
        return true;
    }
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstruct(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoDeserializeFromString(
        const TString& originalData, const TChunkConstructionData& externalInfo) const override;
    virtual TString DoSerializeToString(
        const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const override;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override;

public:
    TConstructor()
        : TBase(IChunkedArray::EType::CompactKVArray) {
    }

    TConstructor(const TSettings& settings)
        : TBase(IChunkedArray::EType::CompactKVArray)
        , Settings(settings) {
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
