#pragma once
#include "data_extractor.h"
#include "partial.h"

#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TConstructor: public IConstructor {
private:
    using TBase = IConstructor;
    std::shared_ptr<IDataAdapter> DataExtractor = std::make_shared<TFirstLevelSchemaData>();
    TSettings Settings;

public:
    static TString GetClassNameStatic() {
        return TGlobalConst::SubColumnsDataAccessorName;
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
        const std::shared_ptr<IChunkedArray>& chunkedArray, const TChunkConstructionData& externalInfo) const override;
    virtual NKikimrArrowAccessorProto::TConstructor DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) override;
    virtual TConclusion<std::shared_ptr<IChunkedArray>> DoConstructDefault(const TChunkConstructionData& externalInfo) const override;

public:
    TConstructor()
        : TBase(IChunkedArray::EType::SubColumnsArray) {
    }

    static TConclusion<std::shared_ptr<TGeneralContainer>> BuildOthersContainer(
        const TStringBuf data, const NKikimrArrowAccessorProto::TSubColumnsAccessor& proto, const TChunkConstructionData& externalInfo, const bool deserialize);

    static TConclusion<std::shared_ptr<TSubColumnsPartialArray>> BuildPartialReader(
        const TString& originalData, const TChunkConstructionData& externalInfo);

    TConstructor(const TSettings& settings)
        : TBase(IChunkedArray::EType::SubColumnsArray)
        , Settings(settings) {
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static TConclusion<ui32> GetHeaderSize(const TString& blob) {
        TStringInput si(blob);
        ui32 protoSize;
        if (blob.size() < sizeof(protoSize)) {
            return TConclusionStatus::Fail("incorrect blob (too small)");
        }
        si.Read(&protoSize, sizeof(protoSize));
        return (ui32)(protoSize + sizeof(protoSize));
    }

    static TConclusion<ui32> GetFullHeaderSize(const TString& blob) {
        TStringInput si(blob);
        ui32 protoSize;
        if (blob.size() < sizeof(protoSize)) {
            return TConclusionStatus::Fail("incorrect blob (too small)");
        }
        si.Read(&protoSize, sizeof(protoSize));
        ui32 currentIndex = sizeof(protoSize);
        NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
        if (!proto.ParseFromArray(blob.data() + currentIndex, protoSize)) {
            return TConclusionStatus::Fail("cannot parse proto");
        }
        return (ui32)(protoSize + sizeof(protoSize) + proto.GetColumnStatsSize() + proto.GetOtherStatsSize());
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
