#pragma once
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/abstract/iterator.h>
#include <ydb/core/tx/columnshard/engines/reader/sys_view/constructor/constructor.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSysView::NChunks {

class TConstructor: public TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats> {
private:
    using TBase = TStatScannerConstructor<NKikimr::NSysView::Schema::PrimaryIndexStats>;

protected:
    virtual std::shared_ptr<NAbstract::TReadStatsMetadata> BuildMetadata(
        const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;

public:
    using TBase::TBase;
};

class TReadStatsMetadata: public NAbstract::TReadStatsMetadata {
private:
    using TBase = NAbstract::TReadStatsMetadata;
    using TSysViewSchema = NKikimr::NSysView::Schema::PrimaryIndexStats;

public:
    using TBase::TBase;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;
};

class TStatsIterator: public NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexStats> {
private:
    class TViewContainer {
    private:
        TString Data;
        std::string STLData;
        arrow::util::string_view View;

    public:
        const arrow::util::string_view& GetView() const {
            return View;
        }

        TViewContainer(const TString& data)
            : Data(data)
            , View(arrow::util::string_view(Data.data(), Data.size())) {
        }

        TViewContainer(const std::string& data)
            : STLData(data)
            , View(arrow::util::string_view(STLData.data(), STLData.size())) {
        }
    };

    mutable THashMap<ui32, TViewContainer> ColumnNamesById;
    mutable THashMap<NPortion::EProduced, TViewContainer> PortionType;
    mutable THashMap<TString, THashMap<ui32, TViewContainer>> EntityStorageNames;
    std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    std::vector<std::shared_ptr<NGroupedMemoryManager::TGroupGuard>> GroupGuards;

    using TBase = NAbstract::TStatsIterator<NKikimr::NSysView::Schema::PrimaryIndexStats>;

    virtual bool IsReadyForBatch() const override {
        return IndexGranules.size() && IndexGranules.front().GetPortions().size() &&
               FetchedAccessors.contains(IndexGranules.front().GetPortions().front()->GetPortionId());
    }

    virtual bool AppendStats(
        const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, NAbstract::TGranuleMetaView& granule) const override;
    virtual ui32 PredictRecordsCount(const NAbstract::TGranuleMetaView& granule) const override;
    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionDataAccessor& portion) const;

    class TApplyResult: public IDataTasksProcessor::ITask {
    private:
        using TBase = IDataTasksProcessor::ITask;
        YDB_READONLY_DEF(std::vector<TPortionDataAccessor>, Accessors);
        NColumnShard::TCounterGuard WaitingCountersGuard;
    public:
        TString GetTaskClassIdentifier() const override {
            return "TApplyResult";
        }

        TApplyResult(const std::vector<TPortionDataAccessor>& accessors, NColumnShard::TCounterGuard&& waitingCountersGuard)
            : TBase(NActors::TActorId())
            , Accessors(accessors)
            , WaitingCountersGuard(std::move(waitingCountersGuard))
        {
        }

        virtual TConclusionStatus DoExecuteImpl() override {
            AFL_VERIFY(false)("event", "not applicable");
            return TConclusionStatus::Success();
        }
        virtual bool DoApply(IDataReader& /*indexedDataRead*/) const override {
            AFL_VERIFY(false);
            return false;
        }
    };

    class TFetchingAccessorAllocation: public NGroupedMemoryManager::IAllocation, public IDataAccessorRequestsSubscriber {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> Guard;
        std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> AccessorsManager;
        std::shared_ptr<TDataAccessorsRequest> Request;
        NColumnShard::TCounterGuard WaitingCountersGuard;
        const NActors::TActorId OwnerId;
        const std::shared_ptr<NReader::TReadContext> Context;

        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& /*selfPtr*/) override {
            Guard = std::move(guard);
            AccessorsManager->AskData(std::move(Request));
            return true;
        }
        virtual void DoOnAllocationImpossible(const TString& errorMessage) override;

        virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
            if (result.HasErrors()) {
                NActors::TActivationContext::AsActorContext().Send(
                    OwnerId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(TConclusionStatus::Fail("cannot fetch accessors")));
            } else {
                AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
                NActors::TActivationContext::AsActorContext().Send(
                    OwnerId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
                                 std::make_shared<TApplyResult>(result.ExtractPortionsVector(), std::move(WaitingCountersGuard))));
            }
        }

    public:
        TFetchingAccessorAllocation(const std::shared_ptr<TDataAccessorsRequest>& request, const ui64 mem, const std::shared_ptr<NReader::TReadContext>& context);
    };

    virtual void Apply(const std::shared_ptr<IApplyAction>& task) override {
        if (IndexGranules.empty()) {
            return;
        }
        auto result = std::dynamic_pointer_cast<TApplyResult>(task);
        AFL_VERIFY(result);
        AFL_VERIFY(result->GetAccessors().size() == 1);
        FetchedAccessors.emplace(result->GetAccessors().front().GetPortionInfo().GetPortionId(), result->GetAccessors().front());
    }

    virtual TConclusionStatus Start() override;

public:
    using TBase::TBase;
};

class TStoreSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TScannerConstructorContext& request) const override {
        return std::make_unique<TConstructor>(request);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromStore>();
    }

public:
    static const inline TFactory::TRegistrator<TStoreSysViewPolicy> Registrator =
        TFactory::TRegistrator<TStoreSysViewPolicy>(TString(::NKikimr::NSysView::StorePrimaryIndexStatsName));
};

class TTableSysViewPolicy: public NAbstract::ISysViewPolicy {
protected:
    virtual std::unique_ptr<IScannerConstructor> DoCreateConstructor(const TScannerConstructorContext& request) const override {
        return std::make_unique<TConstructor>(request);
    }
    virtual std::shared_ptr<NAbstract::IMetadataFiller> DoCreateMetadataFiller() const override {
        return std::make_shared<NAbstract::TMetadataFromTable>();
    }

public:
    static const inline TFactory::TRegistrator<TTableSysViewPolicy> Registrator =
        TFactory::TRegistrator<TTableSysViewPolicy>(TString(::NKikimr::NSysView::TablePrimaryIndexStatsName));
};

}   // namespace NKikimr::NOlap::NReader::NSysView::NChunks
