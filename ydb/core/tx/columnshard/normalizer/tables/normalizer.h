#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TRemovedTablesNormalizer: public NOlap::INormalizerComponent {
public:
    virtual const TString& GetName() const override {
        const static TString name = "TRemovedTablesNormalizer";
        return name;
    }

private:
    class TNormalizerResult;

public:
    virtual TConclusion<std::vector<INormalizerTask::TPtr>> Init(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}   // namespace NKikimr::NOlap
