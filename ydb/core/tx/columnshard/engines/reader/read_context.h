#pragma once
#include "conveyor_task.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
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
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
    const NColumnShard::TConcreteScanCounters Counters;
    YDB_READONLY_DEF(std::shared_ptr<TActorBasedMemoryAccesor>, MemoryAccessor);
    YDB_READONLY(bool, IsInternalRead, false);
public:
    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager,
        const NColumnShard::TConcreteScanCounters& counters,
        std::shared_ptr<NOlap::TActorBasedMemoryAccesor> memoryAccessor, const bool isInternalRead
        );

    TReadContext(const std::shared_ptr<IStoragesManager>& storagesManager, const NColumnShard::TConcreteScanCounters& counters, const bool isInternalRead)
        : StoragesManager(storagesManager)
        , Counters(counters)
        , IsInternalRead(isInternalRead)
    {

    }
};

class IDataReader {
protected:
    TReadContext Context;
    std::shared_ptr<const TReadMetadata> ReadMetadata;
    virtual std::shared_ptr<NBlobOperations::NRead::ITask> DoExtractNextReadTask(const bool hasReadyResults) = 0;
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
    std::shared_ptr<NBlobOperations::NRead::ITask> ExtractNextReadTask(const bool hasReadyResults) {
        return DoExtractNextReadTask(hasReadyResults);
    }
};

}
