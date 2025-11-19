#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NIndexes {

class TDefaultDataExtractor: public IReadDataExtractor {
public:
    static TString GetClassNameStatic() {
        return "DEFAULT";
    }

private:
    static const inline auto Registrator = TFactory::TRegistrator<TDefaultDataExtractor>(GetClassNameStatic());
    virtual void DoSerializeToProto(TProto& /*proto*/) const override {
        return;
    }
    virtual bool DoDeserializeFromProto(const TProto& /*proto*/) override {
        return true;
    }
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& /*jsonInfo*/) override {
        return TConclusionStatus::Success();
    }

    void VisitSimple(
        const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const ui64 hashBase, const TChunkVisitor& visitor) const;

    virtual void DoVisitAll(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray, const TChunkVisitor& chunkVisitor,
        const TRecordVisitor& recordVisitor) const override;

    virtual bool DoCheckForIndex(const NRequest::TOriginalDataAddress& request, ui64* hashBase) const override;
    virtual THashMap<ui64, ui32> DoGetIndexHitsCount(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& dataArray) const override;

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
