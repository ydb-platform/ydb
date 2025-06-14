#pragma once
#include "fetcher.h"

#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NDataFetcher {

class TPortionsDataFetcher;

class TAskAccessorResourcesStep: public IFetchingStep {
private:
    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->Resume();
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessorResources);
        auto request = std::make_shared<TDataAccessorsRequest>(::ToString(fetchingContext->GetInput()->GetConsumer()));
        for (auto&& i : fetchingContext->GetInput()->GetPortions()) {
            request->AddPortion(i);
        }
        const ui64 memory = request->PredictAccessorsMemory(fetchingContext->GetInput()->GetActualSchema());
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        NGroupedMemoryManager::TCompMemoryLimiterOperator::SendToAllocation(
            FetchingContext->GetCurrentContext().GetMemoryProcessId(), 1, 1, { std::make_shared<TSubscriber>(memory, fetchingContext) }, 0);
        return IFetchingStep::EStepResult::Detached;
    }

public:
};

class TAskAccessorsStep: public IFetchingStep {
private:
    class TSubscriber: public IDataAccessorRequestsSubscriber {
    private:
        std::shared_ptr<TPortionsDataFetcher> Fetcher;

    protected:
        virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
            if (result.HasErrors()) {
                Fetcher->OnError("cannot fetch accessors");
            } else {
                auto accessors = result.ExtractPortionsVector();
                AFL_VERIFY(accessors.size() == Fetcher->GetInput().GetPortions().size());
                for (ui32 idx = 0; idx < accessors.size(); ++idx) {
                    AFL_VERIFY(accessors[idx].GetPortionInfo()->GetPortionId() == Fetcher->GetInput().GetPortions()[idx].GetPortionInfo()->GetPortionId());
                }
                Fetcher->MutableCurrentContext().SetPortionAccessors(std::move(accessors));
                Fetcher->Resume();
            }
        }
        virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
            return Default<std::shared_ptr<const TAtomicCounter>>();
        }

    public:
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessors);
        std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>(fetchingContext->GetConsumer());
        for (auto&& i : fetchingContext->GetPortions()) {
            request->AddPortion(i);
        }
        fetchingContext->GetEnvironment()->GetDataAccessorsManager()->AskData(request);
        return IFetchingStep::EStepResult::Detached;
    }

public:
};

class TAskDataResourceStep: public IFetchingStep {
private:
    std::shared_ptr<NReader::NCommon::TColumnsSetIds> ColumnIds;

    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NOlap::NResourceBroker::NSubscribe::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->Resume();
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskDataResources);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetCurrentContext()->GetPortionAccessors();
        ui64 memory = 0;
        for (auto&& i : accessors) {
            if (ColumnIds) {
                memory += i.GetColumnBlobBytes(ColumnIds->GetColumnIds());
            } else {
                memory += i.GetColumnBlobBytes();
            }
        }
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        NGroupedMemoryManager::TCompMemoryLimiterOperator::SendToAllocation(
            FetchingContext->GetCurrentContext().GetMemoryProcessId(), 1, 1, { std::make_shared<TSubscriber>(memory, fetchingContext) }, 0);
        return IFetchingStep::EStepResult::Detached;
    }

public:
    TAskDataResourceStep(const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds) {
    }
};

class TAskDataStep: public IFetchingStep {
private:
    std::shared_ptr<NReader::NCommon::TColumnsSetIds> ColumnIds;

    class TSubscriber: public NOlap::NBlobOperations::NRead::ITask {
    private:
        using TBase = NOlap::NBlobOperations::NRead::ITask;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

    protected:
        virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) override {
            FetchingContext->MutableCurrentContext().SetBlobs(ExtractBlobsData());
            FetchingContext->Resume();
        }
        virtual bool DoOnError(
            const TString& storageId, const NOlap::TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
            FetchingContext->OnError("cannot read blob range: " + storageId + "::" + range.ToString() + "::" + status.GetErrorMessage());
            return false;
        }

    public:
        TSubscriber(
            const std::shared_ptr<TPortionsDataFetcher>& fetchingContext, std::vector<std::shared_ptr<IBlobsReadingAction>>&& readActions)
            : TBase(readActions, fetchingContext->GetConsumer(), fetchingContext->GetExternalTaskId())
            , FetchingContext(fetchingContext) {
        }
    };

    virtual bool DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::ReadBlobs);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetPortionAccessors();
        THashMap<TString, THashSet<TBlobRange>> ranges;
        ui32 idx = 0;
        for (ui32 idx = 0; idx < accessors.size(); ++idx) {
            if (ColumnIds) {
                i.FillBlobRangesByStorage(ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema());
            } else {
                i.FillBlobRangesByStorage(
                    ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema()->GetIndexInfo(), &ColumnIds->GetColumnIds());
            }
        }
        std::vector<std::shared_ptr<IBlobsReadingAction>> readActions;
        for (auto&& i : ranges) {
            auto blobsOperator = fetchingContext->GetEnvironment()->GetStoragesManager()->GetOperatorVerified(i.first);
            auto readBlobs = blobsOperator->StartReadingAction(fetchingContext->GetConsumer());
            for (auto&& br : i.second) {
                readBlobs->AddRange(br);
            }
            readActions.emplace_back(readBlobs);
        }
        if (readActions.size()) {
            TActorContext::AsActorContext().Register(
                new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TSubscriber>(fetchingContext, std::move(readActions))));
            return true;
        } else {
            return false;
        }
    }

public:
    TAskDataResourceStep(const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds) {
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher
