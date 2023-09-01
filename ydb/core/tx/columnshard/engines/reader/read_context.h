#pragma once
#include "conveyor_task.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/columnshard__scan.h>
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/resources/memory.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NOlap {

struct TReadMetadata;

class TActorBasedMemoryAccesor: public TScanMemoryLimiter::IMemoryAccessor {
private:
    using TBase = TScanMemoryLimiter::IMemoryAccessor;
    const NActors::TActorIdentity OwnerId;
protected:
    virtual void DoOnBufferReady() override;
public:
    TActorBasedMemoryAccesor(const NActors::TActorIdentity& ownerId, const TString& limiterName)
        : TBase(TMemoryLimitersController::GetLimiter(limiterName))
        , OwnerId(ownerId) {

    }
};

class TReadContext {
private:
    YDB_ACCESSOR_DEF(NColumnShard::TDataTasksProcessorContainer, Processor);
    const NColumnShard::TConcreteScanCounters Counters;
    YDB_READONLY_DEF(std::shared_ptr<NOlap::TActorBasedMemoryAccesor>, MemoryAccessor);
    YDB_READONLY(bool, IsInternalRead, false);
public:
    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    TReadContext(const NColumnShard::TDataTasksProcessorContainer& processor,
        const NColumnShard::TConcreteScanCounters& counters,
        std::shared_ptr<NOlap::TActorBasedMemoryAccesor> memoryAccessor, const bool isInternalRead
        );

    TReadContext(const NColumnShard::TConcreteScanCounters& counters, const bool isInternalRead)
        : Counters(counters)
        , IsInternalRead(isInternalRead)
    {

    }
};

class IDataReader {
protected:
    TReadContext Context;
    std::shared_ptr<const TReadMetadata> ReadMetadata;
    virtual void DoAddData(const TBlobRange& blobRange, const TString& data) = 0;
    virtual std::optional<TBlobRange> DoExtractNextBlob(const bool hasReadyResults) = 0;
    virtual TString DoDebugString() const = 0;
    virtual void DoAbort() = 0;
    virtual bool DoIsFinished() const = 0;
    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t maxRowsInBatch) = 0;
public:
    IDataReader(const TReadContext& context, std::shared_ptr<const TReadMetadata> readMetadata);
    virtual ~IDataReader() = default;

    const std::shared_ptr<const TReadMetadata>& GetReadMetadata() const {
        return ReadMetadata;
    }

    const TReadContext& GetContext() const {
        return Context;
    }

    TReadContext& GetContext() {
        return Context;
    }

    const std::shared_ptr<TActorBasedMemoryAccesor>& GetMemoryAccessor() const {
        return Context.GetMemoryAccessor();
    }

    const NColumnShard::TConcreteScanCounters& GetCounters() const noexcept {
        return Context.GetCounters();
    }

    const NColumnShard::TDataTasksProcessorContainer& GetTasksProcessor() const noexcept {
        return Context.GetProcessor();
    }

    void Abort() {
        return DoAbort();
    }

    template <class T>
    T& GetMeAs() {
        auto result = dynamic_cast<T*>(this);
        Y_VERIFY(result);
        return *result;
    }

    template <class T>
    const T& GetMeAs() const {
        auto result = dynamic_cast<const T*>(this);
        Y_VERIFY(result);
        return *result;
    }

    std::vector<TPartialReadResult> ExtractReadyResults(const int64_t maxRowsInBatch) {
        return DoExtractReadyResults(maxRowsInBatch);
    }

    bool IsFinished() const {
        return DoIsFinished();
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "internal:" << Context.GetIsInternalRead() << ";"
           << "has_buffer:" << (GetMemoryAccessor() ? GetMemoryAccessor()->HasBuffer() : true) << ";"
            ;
        sb << DoDebugString();
        return sb;
    }

    void AddData(const TBlobRange& blobRange, const TString& data) {
        DoAddData(blobRange, data);
    }
    std::optional<TBlobRange> ExtractNextBlob(const bool hasReadyResults) {
        return DoExtractNextBlob(hasReadyResults);
    }
};

}
