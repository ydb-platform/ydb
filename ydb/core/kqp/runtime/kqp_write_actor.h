#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/core/kqp/common/buffer/events.h>

namespace NKikimr {
namespace NKqp {


// TODO: move somewhere else
class IKqpWriteBuffer {
public:
    virtual ~IKqpWriteBuffer() = default;

    // Only when all writes are closed!
    virtual void Flush(std::function<void()> callback) = 0;
    //virtual void Flush(TTableId tableId) = 0;

    virtual void Prepare(std::function<void(TPreparedInfo&&)> callback, TPrepareSettings&& prepareSettings) = 0;
    virtual void OnCommit(std::function<void(ui64)> callback) = 0;
    virtual void ImmediateCommit(std::function<void(ui64)> callback, ui64 txId) = 0;
    //virtual void Rollback(std::function<void(ui64)> callback) = 0;

    virtual THashSet<ui64> GetShardsIds() const = 0;
    virtual THashMap<ui64, NKikimrDataEvents::TLock> GetLocks() const = 0;

    virtual bool IsFinished() const = 0;

    virtual TActorId GetActorId() const = 0;
};

struct TKqpBufferWriterSettings {
    TActorId SessionActorId;
};

std::pair<IKqpWriteBuffer*, NActors::IActor*> CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings);

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}
