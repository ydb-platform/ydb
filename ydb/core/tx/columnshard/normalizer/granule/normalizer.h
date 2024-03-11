#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

class TGranulesNormalizer: public NOlap::INormalizerComponent {
    class TNormalizerResult;
public:
    virtual const TString& GetName() const override {
        const static TString name = "TGranulesNormalizer";
        return name;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
