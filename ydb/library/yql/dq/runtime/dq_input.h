#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDq {

struct TDqInputStats {
    // basic stats
    ui64 Chunks = 0;
    ui64 Bytes = 0;
    ui64 RowsIn = 0;
    ui64 RowsOut = 0;
    TInstant FirstRowTs;

    // profile stats
    ui64 RowsInMemory = 0;
    ui64 MaxMemoryUsage = 0;
};

class IDqInput : public TSimpleRefCount<IDqInput> {
public:
    using TPtr = TIntrusivePtr<IDqInput>;

    virtual ~IDqInput() = default;

    virtual i64 GetFreeSpace() const = 0;
    virtual ui64 GetStoredBytes() const = 0;

    [[nodiscard]]
    virtual bool Empty() const = 0;

    [[nodiscard]]
    virtual bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) = 0;

    virtual bool IsFinished() const = 0;

    virtual const TDqInputStats* GetStats() const = 0;

    virtual NKikimr::NMiniKQL::TType* GetInputType() const = 0;
    inline TMaybe<ui32> GetInputWidth() const {
        auto type = GetInputType();
        if (type->IsMulti()) {
            return static_cast<const NKikimr::NMiniKQL::TMultiType*>(type)->GetElementsCount();
        }
        return {};
    }

    // Checkpointing
    // After pause IDqInput::Pop() stops return batches that were pushed before pause
    // and returns Empty() after all the data before pausing was read.
    // Compute Actor can push data after pause, but program won't receive it until Resume() is called.
    virtual void Pause() = 0;
    virtual void Resume() = 0;
    virtual bool IsPaused() const = 0;
};

} // namespace NYql::NDq
