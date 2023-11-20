#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

#include "dq_async_stats.h" 

namespace NYql {
namespace NDqProto {

class TCheckpoint;
class TTaskInput;
} // namespace NDqProto

namespace NUdf {
class TUnboxedValue;
} // namespace NUdf

namespace NDq {

struct TDqOutputStats : public TDqAsyncStats {
    // profile stats
    ui64 MaxMemoryUsage = 0;
    ui64 MaxRowsInMemory = 0;
};

class IDqOutput : public TSimpleRefCount<IDqOutput> {
public:
    using TPtr = TIntrusivePtr<IDqOutput>;

    virtual ~IDqOutput() = default;

    virtual const TDqOutputStats& GetPushStats() const = 0;

    // <| producer methods
    [[nodiscard]]
    virtual bool IsFull() const = 0;
    // can throw TDqChannelStorageException
    virtual void Push(NUdf::TUnboxedValue&& value) = 0;
    virtual void WidePush(NUdf::TUnboxedValue* values, ui32 count) = 0;
    virtual void Push(NDqProto::TWatermark&& watermark) = 0;
    // Push checkpoint. Checkpoints may be pushed to channel even after it is finished.
    virtual void Push(NDqProto::TCheckpoint&& checkpoint) = 0;
    virtual void Finish() = 0;

    // <| consumer methods
    [[nodiscard]]
    virtual bool HasData() const = 0;
    virtual bool IsFinished() const = 0;

    virtual NKikimr::NMiniKQL::TType* GetOutputType() const = 0;
};

} // namespace NDq
} // namespace NYql
