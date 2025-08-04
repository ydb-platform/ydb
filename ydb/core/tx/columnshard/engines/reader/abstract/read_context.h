#pragma once
#include "read_metadata.h"

#include <ydb/core/kqp/runtime/scheduler/new/kqp_schedulable_actor.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader {

class TPartialSourceAddress;

class TComputeShardingPolicy {
private:
    YDB_READONLY(ui32, ShardsCount, 0);
    YDB_READONLY_DEF(std::vector<std::string>, ColumnNames);

public:
    TString DebugString() const {
        return TStringBuilder() << "shards_count:" << ShardsCount << ";columns=" << JoinSeq(",", ColumnNames) << ";";
    }

    TComputeShardingPolicy() = default;
    bool DeserializeFromProto(const NKikimrTxDataShard::TComputeShardingPolicy& policy) {
        ShardsCount = policy.GetShardsCount();
        for (auto&& i : policy.GetColumnNames()) {
            ColumnNames.emplace_back(i);
        }
        if (ShardsCount >= 1 && ColumnNames.empty()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("shards_count", ShardsCount)("column_names", JoinSeq(",", ColumnNames));
            return false;
        }
        return true;
    }

    bool IsEnabled() const {
        return ShardsCount > 1 && ColumnNames.size();
    }
};

class TReadContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>, DataAccessorsManager);
    YDB_READONLY_DEF(std::shared_ptr<NColumnFetching::TColumnDataManager>, ColumnDataManager);
    const NColumnShard::TConcreteScanCounters Counters;
    TReadMetadataBase::TConstPtr ReadMetadata;
    NResourceBroker::NSubscribe::TTaskContext ResourcesTaskContext;
    const ui64 ScanId;
    const TActorId ScanActorId;
    const TActorId ResourceSubscribeActorId;
    const TActorId ReadCoordinatorActorId;
    const TComputeShardingPolicy ComputeShardingPolicy;
    std::shared_ptr<TAtomicCounter> AbortionFlag = std::make_shared<TAtomicCounter>(0);
    std::shared_ptr<const TAtomicCounter> ConstAbortionFlag = AbortionFlag;
    const NConveyorComposite::TProcessGuard ConveyorProcessGuard;
    std::shared_ptr<NArrow::NSSA::IColumnResolver> Resolver;

public:
    const NArrow::NSSA::IColumnResolver* GetResolver() const {
        AFL_VERIFY(!!Resolver);
        return Resolver.get();
    }

    ui64 GetConveyorProcessId() const {
        return ConveyorProcessGuard.GetInternalProcessId();
    }

    template <class T>
    std::shared_ptr<const T> GetReadMetadataPtrVerifiedAs() const {
        auto result = dynamic_pointer_cast<const T>(ReadMetadata);
        AFL_VERIFY(result);
        return result;
    }

    const std::shared_ptr<IScanCursor>& GetScanCursor() const {
        return ReadMetadata->GetScanCursor();
    }

    const std::shared_ptr<const TAtomicCounter>& GetAbortionFlag() const {
        return ConstAbortionFlag;
    }

    void AbortWithError(const TString& errorMessage) {
        if (AbortionFlag->Inc() == 1) {
            NActors::TActivationContext::Send(ScanActorId, std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
                                                               TConclusionStatus::Fail(errorMessage), Counters.GetAbortsGuard()));
        }
    }

    void Stop() {
        AbortionFlag->Inc();
    }

    bool IsActive() const {
        return AbortionFlag->Val() == 0;
    }

    bool IsAborted() const {
        return AbortionFlag->Val();
    }

    bool IsReverse() const {
        return ReadMetadata->IsDescSorted();
    }

    const TComputeShardingPolicy& GetComputeShardingPolicy() const {
        return ComputeShardingPolicy;
    }

    const TActorId& GetResourceSubscribeActorId() const {
        return ResourceSubscribeActorId;
    }

    const TActorId& GetReadCoordinatorActorId() const {
        return ReadCoordinatorActorId;
    }

    const TActorId& GetScanActorId() const {
        return ScanActorId;
    }

    ui64 GetScanId() const {
        return ScanId;
    }

    bool HasLock() const {
        return !!ReadMetadata->GetLockId();
    }

    const TReadMetadataBase::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    const NResourceBroker::NSubscribe::TTaskContext& GetResourcesTaskContext() const {
        return ResourcesTaskContext;
    }

    TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager, const NColumnShard::TConcreteScanCounters& counters,
        const TReadMetadataBase::TConstPtr& readMetadata, const TActorId& scanActorId, const TActorId& resourceSubscribeActorId,
        const TActorId& readCoordinatorActorId, const TComputeShardingPolicy& computeShardingPolicy, const ui64 scanId,
        const NConveyorComposite::TCPULimitsConfig& cpuLimits);
};

class IDataReader {
protected:
    std::shared_ptr<TReadContext> Context;
    bool Started = false;
    virtual TConclusionStatus DoStart() = 0;
    virtual TString DoDebugString(const bool verbose) const = 0;
    virtual void DoAbort() = 0;
    virtual bool DoIsFinished() const = 0;
    virtual std::vector<std::unique_ptr<TPartialReadResult>> DoExtractReadyResults(const int64_t maxRowsInBatch) = 0;
    virtual TConclusion<bool> DoReadNextInterval() = 0;

public:
    IDataReader(const std::shared_ptr<TReadContext>& context);
    virtual ~IDataReader() = default;

    TConclusionStatus Start() {
        AFL_VERIFY(!Started);
        Started = true;
        return DoStart();
    }
    virtual void OnSentDataFromInterval(const TPartialSourceAddress& address) = 0;

    const TReadContext& GetContext() const {
        return *Context;
    }

    TReadContext& GetContext() {
        return *Context;
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const noexcept {
        return Context->GetCounters();
    }

    void Abort(const TString& reason) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "scan_aborted")("reason", reason);
        return DoAbort();
    }

    template <class T>
    T& GetMeAs() {
        auto result = dynamic_cast<T*>(this);
        Y_ABORT_UNLESS(result);
        return *result;
    }

    template <class T>
    const T& GetMeAs() const {
        auto result = dynamic_cast<const T*>(this);
        Y_ABORT_UNLESS(result);
        return *result;
    }

    std::vector<std::unique_ptr<TPartialReadResult>> ExtractReadyResults(const int64_t maxRowsInBatch) {
        return DoExtractReadyResults(maxRowsInBatch);
    }

    bool IsFinished() const {
        return DoIsFinished();
    }

    TString DebugString(const bool verbose) const {
        TStringBuilder sb;
        sb << DoDebugString(verbose);
        return sb;
    }
    [[nodiscard]] TConclusion<bool> ReadNextInterval() {
        return DoReadNextInterval();
    }
};

}   // namespace NKikimr::NOlap::NReader
