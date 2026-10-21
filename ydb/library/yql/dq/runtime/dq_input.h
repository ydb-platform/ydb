#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

#include "dq_async_stats.h"

#include <memory>

namespace NYql::NDq {

using TDqInputStats = TDqAsyncStats;

class TDqInputReadySet;

class IDqInput : public TSimpleRefCount<IDqInput> {
public:
    using TPtr = TIntrusivePtr<IDqInput>;

    virtual ~IDqInput() = default;

    virtual const TDqInputStats& GetPopStats() const = 0;
    virtual i64 GetFreeSpace() const = 0;
    virtual ui64 GetStoredBytes() const = 0;

    [[nodiscard]]
    virtual bool Empty() const = 0;

    [[nodiscard]]
    virtual bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>& watermark) = 0;

    virtual bool IsFinished() const = 0;

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
    virtual void PauseByCheckpoint() = 0;
    virtual void ResumeByCheckpoint() = 0;
    virtual bool IsPausedByCheckpoint() const = 0;

    // Opt in to be polled by a union only when marked, see TDqInputReadySet. An input which returns true marks
    // `slot` right away, and then whenever Pop() or IsFinished() may have changed: data, a watermark or a
    // checkpoint arrived, the input finished, or it is resumed after a checkpoint. It marks itself before it wakes
    // the consumer up, and also when its Pop() returns false without the input having been found empty.
    // Binding again replaces the previous binding. A union is either all bound or all polled: if one of its inputs
    // returns false, it polls them all as before. The task runner asks for it only where the inputs support it.
    virtual bool BindReadySet(const std::shared_ptr<TDqInputReadySet>& set, ui32 slot) {
        Y_UNUSED(set, slot);
        return false;
    }
};

} // namespace NYql::NDq
