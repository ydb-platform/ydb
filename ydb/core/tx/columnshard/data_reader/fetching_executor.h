#pragma once
#include <ydb/core/tx/conveyor_composite/usage/common.h>
namespace NKikimr::NOlap::NDataFetcher {

class TPortionsDataFetcher;

class TFetchingExecutor: public NConveyorComposite::ITask {
private:
    std::shared_ptr<TPortionsDataFetcher> Fetcher;
    virtual void DoExecute(const std::shared_ptr<ITask>& taskPtr) override;

public:
    virtual TString GetTaskClassIdentifier() const override;

    TFetchingExecutor(const std::shared_ptr<TPortionsDataFetcher>& fetcher)
        : Fetcher(fetcher) {
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher
