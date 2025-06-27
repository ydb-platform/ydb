#pragma once
#include "fetcher.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
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
        using TBase = NGroupedMemoryManager::IAllocation;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->MutableScript().Next();
            return FetchingContext->Resume(FetchingContext);
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessorResources);
        auto request = std::make_shared<TDataAccessorsRequest>(::ToString(fetchingContext->GetInput().GetConsumer()));
        for (auto&& i : fetchingContext->GetInput().GetPortions()) {
            request->AddPortion(i->GetPortionInfo());
        }
        const ui64 memory = request->PredictAccessorsMemory(fetchingContext->GetInput().GetActualSchema());
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        fetchingContext->AskMemoryAllocation(std::make_shared<TSubscriber>(memory, fetchingContext));
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
                AFL_VERIFY(result.GetPortions().size() == Fetcher->GetInput().GetPortions().size());
                std::vector<TPortionDataAccessor> accessors;
                for (auto&& i : Fetcher->GetInput().GetPortions()) {
                    accessors.emplace_back(result.ExtractPortionAccessorVerified(i->GetPortionInfo()->GetPortionId()));
                }
                Fetcher->MutableCurrentContext().SetPortionAccessors(std::move(accessors));
                Fetcher->MutableScript().Next();
                Fetcher->Resume(Fetcher);
            }
        }
        virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
            return Default<std::shared_ptr<const TAtomicCounter>>();
        }

    public:
        TSubscriber(const std::shared_ptr<TPortionsDataFetcher>& fetcher)
            : Fetcher(fetcher) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskAccessors);
        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(::ToString(fetchingContext->GetInput().GetConsumer()));
        request->RegisterSubscriber(std::make_shared<TSubscriber>(fetchingContext));
        for (auto&& i : fetchingContext->GetInput().GetPortions()) {
            request->AddPortion(i->GetPortionInfo());
        }
        fetchingContext->GetEnvironment().GetDataAccessorsManager()->AskData(request);
        return IFetchingStep::EStepResult::Detached;
    }

public:
};

class TAskDataResourceStep: public IFetchingStep {
private:
    std::shared_ptr<NReader::NCommon::TColumnsSetIds> ColumnIds;
    std::optional<ui64> MemoryUsage;

    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->MutableScript().Next();
            return FetchingContext->Resume(FetchingContext);
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskDataResources);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetCurrentContext().GetPortionAccessors();
        const ui64 memory = fetchingContext->GetNecessaryDataMemory(ColumnIds, accessors);
        if (!memory) {
            return IFetchingStep::EStepResult::Continue;
        }
        fetchingContext->AskMemoryAllocation(std::make_shared<TSubscriber>(memory, fetchingContext));
        return IFetchingStep::EStepResult::Detached;
    }

public:
    TAskDataResourceStep(const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds)
    {
    }
};

class TAskGeneralResourceStep: public IFetchingStep {
private:
    std::shared_ptr<NReader::NCommon::TColumnsSetIds> ColumnIds;
    const ui64 MemoryUsage;

    class TSubscriber: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::shared_ptr<TPortionsDataFetcher> FetchingContext;

        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            FetchingContext->OnError(errorMessage);
        }
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            FetchingContext->MutableCurrentContext().RegisterResourcesGuard(std::move(guard));
            FetchingContext->MutableScript().Next();
            return FetchingContext->Resume(FetchingContext);
        }

    public:
        TSubscriber(const ui64 memory, const std::shared_ptr<TPortionsDataFetcher>& fetchingContext)
            : TBase(memory)
            , FetchingContext(fetchingContext) {
        }
    };

    virtual IFetchingStep::EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::AskGeneralResources);
        fetchingContext->AskMemoryAllocation(std::make_shared<TSubscriber>(MemoryUsage, fetchingContext));
        return IFetchingStep::EStepResult::Detached;
    }

public:
    TAskGeneralResourceStep(const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds, const ui64 memoryUsage)
        : ColumnIds(columnIds)
        , MemoryUsage(memoryUsage)
    {
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
            FetchingContext->MutableScript().Next();
            FetchingContext->Resume(FetchingContext);
        }
        virtual bool DoOnError(
            const TString& storageId, const NOlap::TBlobRange& range, const NOlap::IBlobsReadingAction::TErrorStatus& status) override {
            FetchingContext->OnError("cannot read blob range: " + storageId + "::" + range.ToString() + "::" + status.GetErrorMessage());
            return false;
        }

    public:
        TSubscriber(
            const std::shared_ptr<TPortionsDataFetcher>& fetchingContext, std::vector<std::shared_ptr<IBlobsReadingAction>>&& readActions)
            : TBase(readActions, ::ToString(fetchingContext->GetInput().GetConsumer()), fetchingContext->GetInput().GetExternalTaskId())
            , FetchingContext(fetchingContext) {
        }
    };

    virtual EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const override {
        fetchingContext->SetStage(EFetchingStage::ReadBlobs);
        const std::vector<TPortionDataAccessor>& accessors = fetchingContext->GetCurrentContext().GetPortionAccessors();
        THashMap<TString, THashSet<TBlobRange>> ranges;
        for (ui32 idx = 0; idx < accessors.size(); ++idx) {
            if (ColumnIds) {
                accessors[idx].FillBlobRangesByStorage(ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema()->GetIndexInfo());
            } else {
                accessors[idx].FillBlobRangesByStorage(
                    ranges, fetchingContext->GetInput().GetPortions()[idx]->GetSchema()->GetIndexInfo(), &ColumnIds->GetColumnIds());
            }
        }
        std::vector<std::shared_ptr<IBlobsReadingAction>> readActions;
        for (auto&& i : ranges) {
            auto blobsOperator = fetchingContext->GetEnvironment().GetStoragesManager()->GetOperatorVerified(i.first);
            auto readBlobs = blobsOperator->StartReadingAction(fetchingContext->GetInput().GetConsumer());
            for (auto&& br : i.second) {
                readBlobs->AddRange(br);
            }
            readActions.emplace_back(readBlobs);
        }
        if (readActions.size()) {
            TActorContext::AsActorContext().Register(
                new NOlap::NBlobOperations::NRead::TActor(std::make_shared<TSubscriber>(fetchingContext, std::move(readActions))));
            return IFetchingStep::EStepResult::Detached;
        } else {
            return IFetchingStep::EStepResult::Continue;
        }
    }

public:
    TAskDataStep(const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds)
        : ColumnIds(columnIds) {
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher
