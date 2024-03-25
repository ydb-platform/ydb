#pragma once
#include "read_metadata.h"
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader {

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
    const NColumnShard::TConcreteScanCounters Counters;
    TReadMetadataBase::TConstPtr ReadMetadata;
    NResourceBroker::NSubscribe::TTaskContext ResourcesTaskContext;
    const TActorId ScanActorId;
    const TActorId ResourceSubscribeActorId;
    const TActorId ReadCoordinatorActorId;
    const TComputeShardingPolicy ComputeShardingPolicy;
public:
    template <class T>
    std::shared_ptr<const T> GetReadMetadataPtrVerifiedAs() const {
        auto result = dynamic_pointer_cast<const T>(ReadMetadata);
        AFL_VERIFY(result);
        return result;
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

    const TReadMetadataBase::TConstPtr& GetReadMetadata() const {
        return ReadMetadata;
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    const NResourceBroker::NSubscribe::TTaskContext& GetResourcesTaskContext() const {
        return ResourcesTaskContext;
    }

    TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager, const NColumnShard::TConcreteScanCounters& counters, const TReadMetadataBase::TConstPtr& readMetadata,
        const TActorId& scanActorId, const TActorId& resourceSubscribeActorId, const TActorId& readCoordinatorActorId, const TComputeShardingPolicy& computeShardingPolicy)
        : StoragesManager(storagesManager)
        , Counters(counters)
        , ReadMetadata(readMetadata)
        , ResourcesTaskContext("CS::SCAN_READ", counters.ResourcesSubscriberCounters)
        , ScanActorId(scanActorId)
        , ResourceSubscribeActorId(resourceSubscribeActorId)
        , ReadCoordinatorActorId(readCoordinatorActorId)
        , ComputeShardingPolicy(computeShardingPolicy)
    {
        Y_ABORT_UNLESS(ReadMetadata);
    }
};

class IDataReader {
protected:
    std::shared_ptr<TReadContext> Context;
    virtual TString DoDebugString(const bool verbose) const = 0;
    virtual void DoAbort() = 0;
    virtual bool DoIsFinished() const = 0;
    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t maxRowsInBatch) = 0;
    virtual bool DoReadNextInterval() = 0;
public:
    IDataReader(const std::shared_ptr<TReadContext>& context);
    virtual ~IDataReader() = default;

    const TReadContext& GetContext() const {
        return *Context;
    }

    TReadContext& GetContext() {
        return *Context;
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const noexcept {
        return Context->GetCounters();
    }

    void Abort() {
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

    std::vector<TPartialReadResult> ExtractReadyResults(const int64_t maxRowsInBatch) {
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
    bool ReadNextInterval() {
        return DoReadNextInterval();
    }
};

}
