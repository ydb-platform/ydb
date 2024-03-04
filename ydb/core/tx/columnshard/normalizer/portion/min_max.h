#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

class TPortionsNormalizer : public INormalizerComponent {
public:
    class TNormalizerResult;

public:
    TPortionsNormalizer(TTabletStorageInfo* info)
        : DsGroupSelector(info)
    {}

    virtual const TString& GetName() const override {
        const static TString name = "TPortionsNormalizer";
        return name;
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    NColumnShard::TBlobGroupSelector DsGroupSelector;
};

}
