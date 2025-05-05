#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NIndexes {

class TSubColumnDataExtractor: public IReadDataExtractor {
public:
    static TString GetClassNameStatic() {
        return "SUB_COLUMN";
    }

private:
    static const inline auto Registrator = TFactory::TRegistrator<TSubColumnDataExtractor>(GetClassNameStatic());

    TString SubColumnName;

    virtual void DoSerializeToProto(TProto& proto) const override {
        AFL_VERIFY(!!SubColumnName);
        proto.MutableSubColumn()->SetSubColumnName(SubColumnName);
    }
    virtual bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasSubColumn()) {
            return false;
        }
        SubColumnName = proto.GetSubColumn().GetSubColumnName();
        if (!SubColumnName) {
            return false;
        }
        return true;
    }
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        if (!jsonInfo.Has("sub_column_name")) {
            return TConclusionStatus::Fail("extractor description has to have 'sub_column_name' parameter");
        }
        if (!jsonInfo["sub_column_name"].IsString()) {
            return TConclusionStatus::Fail("extractor description parameter 'sub_column_name' has to been string");
        }
        SubColumnName = jsonInfo["sub_column_name"].GetString();
        if (!SubColumnName) {
            return TConclusionStatus::Fail("extractor description parameter 'sub_column_name' hasn't been empty");
        }
        return TConclusionStatus::Success();
    }

    virtual void DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
        const TRecordVisitor& recordVisitor) const override;

    virtual bool DoCheckForIndex(const NRequest::TOriginalDataAddress& request, ui64* /*hashBase*/) const override {
        return request.GetSubColumnName() == SubColumnName;
    }

    virtual THashMap<ui64, ui32> DoGetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
