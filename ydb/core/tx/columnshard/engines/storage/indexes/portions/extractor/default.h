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

    virtual std::vector<NRequest::TOriginalDataAddress> DoGetOriginalDataAddresses(const std::set<ui32>& columnIds) const override {
        std::vector<NRequest::TOriginalDataAddress> result;
        for (auto&& i : columnIds) {
            result.emplace_back(NRequest::TOriginalDataAddress(i));
        }
        return result;
    }

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
